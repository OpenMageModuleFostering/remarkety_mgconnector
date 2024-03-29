<?php
/**
 * @copyright    Copyright (C) 2016 InteraMind Ltd (Remarkety). All rights reserved.
 * @license    http://www.gnu.org/licenses/gpl-2.0.html GNU/GPL
 * @package    This file is part of Magento Connector Plugin for Remarkety
 **/

define('REMARKETY_MGCONNECTOR_STATUS', 'STATUS');
define('REMARKETY_MGCONNECTOR_CALLED_STATUS', 'CALLED');
define('REMARKETY_MGCONNECTOR_SUCCESS_STATUS', 'SUCCESS');
define('REMARKETY_MGCONNECTOR_FAILED_STATUS', 'FAILED');
define('REMARKETY_MGCONNECTOR_ERROR', 'ERROR');
define('REMARKETY_MGCONNECTOR_DATA', 'DATA');
define('REMARKETY_MGCONNECTOR_MAGE_VERSION', 'MAGENTO_VERSION');
define('REMARKETY_MGCONNECTOR_EXT_VERSION', 'PLUGIN_VERSION');
define('REMARKETY_EXECUTION_TIME', 'EXECUTION_TIME_MS');
if (!defined("REMARKETY_LOG"))
    define('REMARKETY_LOG', 'remarkety_mgconnector.log');
define('REMARKETY_LOG_SEPARATOR', '|');

class Remarkety_Mgconnector_Model_Core extends Mage_Core_Model_Abstract
{

    const XPATH_CATEGORIES_TO_IGNORE = 'remarkety/mgconnector/categories-to-ignore';
    private $_categoriesToIgnore = array();

    private $configurable_product_model = null;

    private $_productCache = array();
    private $_categoryCache = array();

    private $_sendLogInResponse = false;
    private $_log = array();

    private $_startTime = 0;

    private $_groupedTypes = array('configurable', 'grouped');
    private $simpleProductsStandalone = false;

    private $response_mask = array(
        'customer' => array(
            'entity_id',
            'dob',
            'firstname',
            'lastname',
            'email',
            'created_at',
            'updated_at',
            'is_active',
            'default_billing',
            'default_shipping',
            'registered_to_newsletter',
            'group_id',
            'group_name',
            'gender',
            'prefix',
            'rewards_points',
            'store_credits',
            'address' => array(
                'created_at',
                'updated_at',
                'is_active',
                'firstname',
                'lastname',
                'city',
                'country_id',
                'region',
                'postcode',
                'region_id',
                'street',
                'company',
                'telephone'
            )
        ),
        'subscriber' => array(
            'subscriber_id',
            'change_status_at',
            'customer_id',
            'subscriber_email',
            'subscriber_status'
        ),
        'quote' => array(
            'entity_id',
            'created_at',
            'updated_at',
            'converted_at',
            'customer_id',
            'customer_is_guest',
            'customer_email',
            'customer_firstname',
            'customer_lastname',
            'is_active',
            'grand_total',
            'discount_amount',
            'base_subtotal',
            'base_subtotal_with_discount',
            'coupon_code',
            'quote_currency_code',
            'customer_registered_to_newsletter',
            'currency',
            'checkout_url',
            'items' => array(
                '*' => array(
                    'item_id',
                    'parent_item_id',
                    'categories',
                    'qty',
                    'price',
                    'price_incl_tax',
                    'base_price',
                    'base_price_incl_tax',
                    'name',
                    'sku',
                    'created_at',
                    'updated_at',
                    'product_id',
                    'type_id',
                    'url_path'
                )
            )
        ),
        'order' => array(
            'rem_main_increment_id',
            'increment_id',
            'entity_id',
            'grand_total',
            'shipping_amount',
            'coupon_code',
            'created_at',
            'updated_at',
            'order_currency_code',
            'customer_id',
            'customer_is_guest',
            'customer_email',
            'customer_firstname',
            'customer_lastname',
            'customer_group_id',
            'customer_group',
            'state',
            'status',
            'address',
            'currency',
            'discount_amount',
            'customer_registered_to_newsletter',
            'payment_method',
            'items' => array(
                '*' => array(
                    'parent_item_id',
                    'product_id',
                    'qty_ordered',
                    'price',
                    'price_incl_tax',
                    'base_price',
                    'base_price_incl_tax',
                    'sku',
                    'name',
                    'created_at',
                    'updated_at',
                    'base_image',
                    'thumbnail_image',
                    'small_image',
                    'categories',
                    'type_id',
                    'url_path'
                )
            )
        ),
        'product' => array(
            'entity_id',
            'sku',
            'name',
            'created_at',
            'updated_at',
            'type_id',
            'base_image',
            'thumbnail_image',
            'is_salable',
            'visibility',
            'categories',
            'small_image',
            'price',
            'special_price',
            'cost',
            'url_path',
            'is_in_stock',
            'inventory',
            'parent_id'
        )
    );

    private function _log($function, $status, $message, $data, $logLevel = null)
    {
        $logMsg = implode(REMARKETY_LOG_SEPARATOR, array($function, $status, $message, json_encode($data)));
//        $force = ($status != REMARKETY_MGCONNECTOR_CALLED_STATUS);
        if ($this->_sendLogInResponse)
            $this->_log[] = $logLevel . ": " . $logMsg;
        Mage::log($logMsg, $logLevel, REMARKETY_LOG);
    }

    private function _debug($function, $status, $message, $data)
    {
        $logMsg = implode(REMARKETY_LOG_SEPARATOR, array($function, $status, $message, json_encode($data)));
//        $force = ($status != REMARKETY_MGCONNECTOR_CALLED_STATUS);
        if ($this->_sendLogInResponse)
            $this->_log[] = Zend_Log::DEBUG . ": " . $logMsg;
        Mage::log($logMsg, Zend_Log::DEBUG, REMARKETY_LOG);
    }

    private function _wrapResponse($data, $status, $statusStr = null)
    {
        $ret = array();

        $ret[REMARKETY_MGCONNECTOR_DATA] = $data;
        $ret[REMARKETY_MGCONNECTOR_STATUS] = $status;
        $ret[REMARKETY_MGCONNECTOR_ERROR] = $statusStr;
        $ret[REMARKETY_MGCONNECTOR_EXT_VERSION] = (string)Mage::getConfig()->getNode()->modules->Remarkety_Mgconnector->version;
        $ret[REMARKETY_MGCONNECTOR_MAGE_VERSION] = Mage::getVersion();
        $ret[REMARKETY_EXECUTION_TIME] = (microtime(true) - $this->_startTime) * 1000;
        if ($this->_sendLogInResponse)
            $ret["log"] = $this->_log;
        return $ret;
    }

    private function _store_views_in_group($group_id)
    {
        if (empty($group_id)) {
            return array(-1);
        } else {
            $group = Mage::getModel('core/store_group')->load($group_id);
            if (empty($group)) return array(-1);

            $codes = array_keys($group->getStoreCodes());
            return $codes;
        }
    }

    private function _filter_output_data($data, $field_set = array())
    {
        if (empty($field_set)) return $data;

        foreach (array_keys($data) as $key) {
            if (isset($field_set[$key]) && is_array($field_set[$key])) {
                $data[$key] = $this->_filter_output_data($data[$key], $field_set[$key]);
            } else if (isset($field_set['*']) && is_array($field_set['*'])) {
                $data[$key] = $this->_filter_output_data($data[$key], $field_set['*']);
            } else {
                if (!in_array($key, $field_set)) unset ($data[$key]);
            }
        }
        return $data;
    }

    public function _productCategories(Mage_Catalog_Model_Product $product, $getFromParent = true)
    {
        $categoryCollection = $product->getCategoryCollection()
            ->addAttributeToSelect('name');

        $categories = array();

        foreach ($categoryCollection as $category) {
            $categoryId = $category->getId();
            if (!in_array($categoryId, $this->_categoriesToIgnore)) {
                if (!array_key_exists($categoryId, $this->_categoryCache)) {
                    $pathInStore = array_reverse(explode(",", $category->getPathInStore()));
                    $parentCategories = $category->getParentCategories();   // Uses a single DB query
                    $categoryTree = array();
                    foreach ($pathInStore as $parentCategoryId) {
                        foreach ($parentCategories as $parentCategory) {
                            if (!in_array($parentCategoryId, $this->_categoriesToIgnore)) {
                                if ($parentCategory->getId() == $parentCategoryId) {
                                    $parentCategoryName = $parentCategory->getName();
                                    if (!empty($parentCategoryName) && !in_array($parentCategoryName, $categoryTree)) {
                                        $categoryTree[] = $parentCategoryName;
                                    }
                                }
                            }
                        }
                    }
                    $this->_categoryCache[$categoryId] = implode(" / ", $categoryTree);
                }
                if(!empty($this->_categoryCache[$categoryId]))
                    $categories[] = $this->_categoryCache[$categoryId];
            }
        }
        //if no categories found, get from parent product
        if ($getFromParent && empty($categories)) {
            $parent_id = $this->getProductParentId($product);
            if ($parent_id !== false) {
                $parentProduct = $this->loadProduct($parent_id);
                return $this->_productCategories($parentProduct, false);
            }
        }

        return $categories;
    }

    private function _currencyInfo($currencyCode)
    {
        $currencyInfo = array(
            'code' => Mage::app()->getLocale()->currency($currencyCode)->getShortName(),
            'name' => Mage::app()->getLocale()->currency($currencyCode)->getName(),
            'symbol' => Mage::app()->getLocale()->currency($currencyCode)->getSymbol()
        );

        return $currencyInfo;
    }

    public function __construct()
    {
        $this->_startTime = microtime(true);
        $collection = Mage::getModel('core/config_data')->getCollection();
        $collection
            ->getSelect()
            ->where('path = ?', self::XPATH_CATEGORIES_TO_IGNORE);
        foreach ($collection as $config) {
            $this->_categoriesToIgnore = array_merge($this->_categoriesToIgnore, explode(",", $config->getValue()));
        }
    }

    public function getSubscribers($mage_store_view_id = null, $limit = null, $page = null, $sinceId = null)
    {
        register_shutdown_function('handleShutdown');
        $ret = array();
        $pageNumber = null;
        $pageSize = null;
        $myArgs = func_get_args();
        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            $ret = array();
            $subscriberCollection = Mage::getModel('newsletter/subscriber')
                ->getCollection()
                ->addOrder('subscriber_id', 'ASC');

            // Get only subscribers who are not shoppers
            $subscriberCollection->addFieldToFilter('customer_id', array('eq' => 0));

            $store_views = $this->_store_views_in_group($mage_store_view_id);
            $subscriberCollection->addFieldToFilter('store_id', array('in' => $store_views));
            if ($limit != null) {
                $pageNumber = 1;        // Note that page numbers begin at 1 in Magento
                $pageSize = $limit;
            }

            if ($page != null) {
                if (!is_null($pageSize)) {
                    $pageNumber = $page + 1;    // Note that page numbers begin at 1 in Magento
                }
            }

            if (!is_null($pageSize)) {
                $subscriberCollection->setPageSize($pageSize)
                    ->setCurPage($pageNumber);
            }

            if (!empty($sinceId)) {
                $subscriberCollection->addFieldToFilter('subscriber_id', array('gt' => (int)$sinceId));
            }

            foreach ($subscriberCollection as $subscriber) {
                $subscriberData = $subscriber->toArray();

                $subscriberData = $this->_filter_output_data($subscriberData, $this->response_mask['subscriber']);
                $ret[] = $subscriberData;
            }


            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function getSubscriberCount($mage_store_view_id)
    {
        register_shutdown_function('handleShutdown');
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            $store_views = $this->_store_views_in_group($mage_store_view_id);

            $subscriberCollection = Mage::getModel('newsletter/subscriber')
                ->getCollection();

            // Get only subscribers who are not shoppers
            $subscriberCollection->addFieldToFilter('customer_id', array('eq' => 0));

            $store_views = $this->_store_views_in_group($mage_store_view_id);
            $subscriberCollection->addFieldToFilter('store_id', array('in' => $store_views));

            $count = $subscriberCollection->getSize();
            $ret = array('count' => $count);

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }


    public function getCustomers(
        $mage_store_view_id,
        $updated_at_min = null,
        $updated_at_max = null,
        $limit = null,
        $page = null,
        $since_id = null)
    {
        register_shutdown_function('handleShutdown');
        $ret = array();
        $pageNumber = null;
        $pageSize = null;
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            $customersCollection = Mage::getModel("customer/customer")
                ->getCollection()
                ->addOrder('updated_at', 'ASC')
                ->addAttributeToSelect('*');

            $extensionHelper = Mage::helper('mgconnector/extension');
            $rewardPointsInstance = $extensionHelper
                ->getRewardPointsIntegrationInstance();
            if ($rewardPointsInstance !== false) {
                //add reward points join
                $rewardPointsInstance->modifyCustomersCollection($customersCollection);
            }

            /**
             * @var $store Mage_Core_Model_Store
             */
            $store = Mage::getModel('core/store')->load($mage_store_view_id);
            $website = $store->getWebsite();
            $stores_in_website = $website->getStoreIds();
            $singleStore = !empty($stores_in_website) &&
                            count($stores_in_website) === 1 &&
                            isset($stores_in_website[$mage_store_view_id]) &&
                            $stores_in_website[$mage_store_view_id] == $mage_store_view_id;

            $customerShare = Mage::helper('mgconnector/configuration')->allowCustomerSharing();
            if($customerShare){
                $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, 'Customer account sharing is enabled, not filtering by store', '');
            } elseif($singleStore) {
                //returns all customers for the website, includes admin (store_id = 0)
                $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, 'Single store in website - using website_id filter', '');
                $customersCollection->addFieldToFilter('website_id', array('eq' => $website->getId()));
            } else {
                //returns only customer in a specific store view
                $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, 'Multi store in website - using store_id filter', '');
                $customersCollection->addFieldToFilter('store_id', array('eq' => $mage_store_view_id));
            }
            if ($updated_at_min != null) {
                $customersCollection->addAttributeToFilter('updated_at', array('gt' => $updated_at_min));
            }

            if ($updated_at_max != null) {
                $customersCollection->addAttributeToFilter('updated_at', array('lt' => $updated_at_max));
            }

            if ($since_id != null) {
                $customersCollection->addAttributeToFilter('entity_id', array('gt' => $since_id));
            }

            if ($limit != null) {
                $pageNumber = 1;        // Note that page numbers begin at 1 in Magento
                $pageSize = $limit;
            }

            if ($page != null) {
                if (!is_null($pageSize)) {
                    $pageNumber = $page + 1;    // Note that page numbers begin at 1 in Magento
                }
            }

            if (!is_null($pageSize)) {
                $customersCollection->setPage($pageNumber, $pageSize);
            }

            $subscriberModel = Mage::getModel("newsletter/subscriber");
            $groupModel = Mage::getModel("customer/group");

            /**
             *
             * Special code to exclude Amazon's group shoppers
             * You can un-comment this code and change the group name ("amazon") to fit your needs
             */
            /*
            $ignoredGroupIds = $this->getIgnoredGroupIds("amazon");
            if (!empty($ignoredGroupIds))
                $customersCollection->addAttributeToFilter("group_id", array("nin"=>$ignoredGroupIds));
             */

            foreach ($customersCollection as $customer) {
                $customerData = $customer->toArray();

                $subscriberModel->loadByCustomer($customer);
                $customerData['registered_to_newsletter'] = $subscriberModel->isSubscribed();

                $groupModel->load($customer->getGroupId());
                $groupName = $groupModel->getCustomerGroupCode();
                $customerData ['group_name'] = $groupName;
                $customerData ['group_id'] = $customer->getGroupId();

                $addresses_coll = $customer->getAddressesCollection();
                $addresses_array = $addresses_coll->toArray();

                if (is_array($addresses_array) && !empty($addresses_array)) {
                    if (in_array('default_billing', $customerData) && array_key_exists($customerData['default_billing'], $addresses_array)) {
                        $customerData['address_origin'] = 'billing';
                        $customerData['address'] = $addresses_array[$customerData['default_billing']];
                    } else if (in_array('default_shipping', $customerData) && array_key_exists($customerData['default_shipping'], $addresses_array)) {
                        $customerData['address_origin'] = 'shipping';
                        $customerData['address'] = $addresses_array[$customerData['default_shipping']];
                    } else {
                        $customerData['address_origin'] = 'first';
                        $customerData['address'] = array_shift($addresses_array);
                    }
                }

                $customerData = $this->_filter_output_data($customerData, $this->response_mask['customer']);
                $ret[] = $customerData;
            }

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function getOrders(
        $mage_store_view_id,
        $updated_at_min = null,
        $updated_at_max = null,
        $limit = null,
        $page = null,
        $since_id = null,
        $created_at_min = null,
        $created_at_max = null,
        $order_status = null,    // not implemented
        $order_id = null
    )
    {

        register_shutdown_function('handleShutdown');
        $orders = array();
        $pageNumber = null;
        $pageSize = null;
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            $ordersCollection = Mage::getModel("sales/order")
                ->getCollection()
                ->addOrder('updated_at', 'ASC')
                ->addAttributeToSelect('*');

//            $store_views = $this->_store_views_in_group($mage_store_view_id);
//            $ordersCollection->addFieldToFilter('store_id', array('in' => $store_views));
            $ordersCollection->addFieldToFilter('main_table.store_id', array('eq' => $mage_store_view_id));
            if ($updated_at_min != null) {
                $ordersCollection->addAttributeToFilter('main_table.updated_at', array('gt' => $updated_at_min));
            }

            if ($updated_at_max != null) {
                $ordersCollection->addAttributeToFilter('main_table.updated_at', array('lt' => $updated_at_max));
            }

            if ($since_id != null) {
                $ordersCollection->addAttributeToFilter('main_table.entity_id', array('gt' => $since_id));
            }

            if ($created_at_min != null) {
                $ordersCollection->addAttributeToFilter('main_table.created_at', array('gt' => $created_at_min));
            }

            if ($created_at_max != null) {
                $ordersCollection->addAttributeToFilter('main_table.created_at', array('lt' => $created_at_max));
            }

            if ($order_id != null) {
                $ordersCollection->addAttributeToFilter('main_table.entity_id', $order_id);
            }

            if ($limit != null) {
                $pageNumber = 1;        // Note that page numbers begin at 1
                $pageSize = $limit;
            }

            if ($page != null) {
                if (!is_null($pageSize)) {
                    $pageNumber = $page + 1;    // Note that page numbers begin at 1
                }
            }

            if (!is_null($pageSize)) {
                $ordersCollection->setPage($pageNumber, $pageSize);
            }

            $subscriberModel = Mage::getModel("newsletter/subscriber");
            $groupModel = Mage::getModel("customer/group");

            /**
             * Special code to exclude Amazon's group shoppers
             * You can un-comment this code and change the group name ("amazon") to fit your needs
             */
            /*
            $ignoredGroupIds = $this->getIgnoredGroupIds("amazon");
            if (!empty($ignoredGroupIds))
                $ordersCollection->addAttributeToFilter("customer_group_id", array("nin"=>$ignoredGroupIds));
            */

            $productIdsToLoad = array();
            $orderIds = array();

            foreach ($ordersCollection as $order) {
                try {
                    $orderData = $order->toArray();
                    if (!empty($orderData['relation_child_id'])) {
                        continue;
                    }
                    $orderIds[] = $orderData['entity_id'];
                    $orderData['rem_main_increment_id'] = (empty($orderData['original_increment_id'])) ? $orderData['increment_id'] : $orderData['original_increment_id'];
                    $orderData['currency'] = $this->_currencyInfo($orderData['order_currency_code']);

                    $subscriberModel->loadByEmail($orderData['customer_email']);
                    $orderData['customer_registered_to_newsletter'] = $subscriberModel->isSubscribed();

                    $addressID = null;

                    if (isset($orderData['billing_address_id']) && $orderData['billing_address_id']) {
                        $addressID = $orderData['billing_address_id'];
                    } elseif (isset($orderData['shipping_address_id']) && $orderData['shipping_address_id']) {
                        $addressID = $orderData['shipping_address_id'];
                    }

                    if (!empty($addressID)) {
                        $address = Mage::getModel('sales/order_address')->load($addressID)->toArray();
                        $orderData['address'] = $address;
                    }

                    $storeUrl = Mage::getBaseUrl(Mage_Core_Model_Store::URL_TYPE_WEB);

//                    $itemsCollection = $order->getItemsCollection();
//                $itemsCollection->join(
//
//                );
                    $itemsCollection = $order->getAllVisibleItems();

                    foreach ($itemsCollection as $item) {
                        $product = null;

                        try {
                            $itemData = $item->toArray();
                            $productIdsToLoad[] = $itemData['product_id'];
                            /* The item data is loaded later and populated for performance reasons - get all the products at once instead of one-by-one in a loop */
                            $orderData['items'][$itemData['item_id']] = $itemData;

                        } catch (Exception $e) {
                            $this->_log(__FUNCTION__, 'Error in handling order items for order ID ' . $orderData['entity_id'], $e->getMessage(), $myArgs);
                        }
                    }

                    $groupModel->load($orderData['customer_group_id']);
                    $groupName = $groupModel->getCustomerGroupCode();
                    $orderData['customer_group'] = $groupName;


                    $orders[] = $orderData;
                } catch (Exception $e) {
                    $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, 'Fetch order failed. continue to next order - ' . $e->getMessage(), $myArgs);
                    continue;
                }
            }

            //load all payment methods at once
            $paymentMethods = $this->loadPaymentMethods($orderIds);

            // Load the products all at once and populate the items
            $this->loadProducts($mage_store_view_id, $productIdsToLoad);

            $ret = array();
            foreach ($orders as &$orderData) {
                try {
                    if (array_key_exists('items', $orderData)) {
                        foreach ($orderData['items'] as &$itemData) {
                            try {
                                $product = $this->loadProduct($itemData['product_id']);
                                if (empty($product)) {
                                    $this->debug("Could not load product, productID: " . $itemData['product_id']);
                                    continue;
                                }

//                        $product->setStoreId($mage_store_view_id)->load($product->getId());
                                $itemData['base_image'] = $this->getImageUrl($product, 'image', $mage_store_view_id);
                                $itemData['small_image'] = $this->getImageUrl($product, 'small', $mage_store_view_id);
                                $itemData['thumbnail_image'] = $this->getImageUrl($product, 'thumbnail', $mage_store_view_id);

                                $itemData['name'] = $this->getProductName($product, $mage_store_view_id);

                                $itemData['type_id'] = $product->getData('type_id');
                                //$itemData['categories'] = $this->_productCategories($product);
                                $prodUrl = $this->getProdUrl($product, $storeUrl, $mage_store_view_id);
                                $itemData['url_path'] = $prodUrl;
                            } catch (Exception $e) {
                                $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, 'Handle order items data failed. continue to next item - ' . $e->getMessage(), $myArgs);
                                continue;
                            }
                        }
                    }
                    //set payment method
                    if (!empty($orderData['entity_id'])) {
                        if (isset($paymentMethods[$orderData['entity_id']])) {
                            $orderData['payment_method'] = $paymentMethods[$orderData['entity_id']];
                        }
                    }
                    $ret[] = $this->_filter_output_data($orderData, $this->response_mask['order']);
                } catch (Exception $e) {
                    $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, 'Handle order failed. continue to next order - ' . $e->getMessage(), $myArgs);
                    continue;
                }
            }


            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function getQuotes(
        $mage_store_view_id,
        $updated_at_min = null,
        $updated_at_max = null,
        $limit = null,
        $page = null,
        $since_id = null)
    {
        register_shutdown_function('handleShutdown');
        $pageNumber = null;
        $pageSize = null;
        $ret = array();
        $myArgs = func_get_args();

        try {
            $quotesCollection = Mage::getResourceModel('reports/quote_collection');

            $quotesCollection->addFieldToFilter('items_count', array('neq' => '0'))
                ->addFieldToFilter('main_table.is_active', '1')
                ->addFieldToFilter('main_table.customer_email', array('notnull' => true))
                ->addSubtotal(array($mage_store_view_id), null)
                ->setOrder('updated_at');
            if (is_numeric($mage_store_view_id)) {
                $quotesCollection->addFieldToFilter('store_id', array('eq' => $mage_store_view_id));
            }

            if ($updated_at_min != null) {
                $quotesCollection->addFieldToFilter('main_table.updated_at', array('gt' => $updated_at_min));
            }

            if ($updated_at_max != null) {
                $quotesCollection->addFieldToFilter('main_table.updated_at', array('lt' => $updated_at_max));
            }

            if ($since_id != null) {
                $quotesCollection->addFieldToFilter('main_table.entity_id', array('gt' => $since_id));
            }

            if ($limit != null) {
                $pageNumber = 1;        // Note that page numbers begin at 1
                $pageSize = $limit;
            }

            if ($page != null) {
                if (!is_null($pageSize)) {
                    $pageNumber = $page + 1;    // Note that page numbers begin at 1
                }
            }

            if (!is_null($pageSize)) {
                $quotesCollection->setPageSize($pageSize)->setCurPage($pageNumber);
            }

            //$storeUrl = Mage::getBaseUrl(Mage_Core_Model_Store::URL_TYPE_WEB);
            $this->_debug(__FUNCTION__, " collection SQL: " . $quotesCollection->getSelect()->__toString(), null, null);

            foreach ($quotesCollection as $quote) {
                $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, "inside carts loop", null);
                $quoteData = $quote->toArray();

                if (empty($quoteData['customer_email']))
                    continue;

                $currency = $quoteData['quote_currency_code'];
                $quoteData['currency'] = $this->_currencyInfo($currency);

                $quoteData['checkout_url'] = $this->getCartRecoveryURL($quoteData['entity_id'], $mage_store_view_id);

                foreach ($quote->getItemsCollection() as $item) {
                    $quoteItem = array();

                    foreach ($this->response_mask['quote']['items']['*'] as $field) {
                        $quoteItem[$field] = $item->getData($field);
                    }
                    $_product = $this->loadProduct($quoteItem['product_id']);
                    //$_product->setStoreId($mage_store_view_id);
                    if (!empty($_product)) {
                        $quoteItem['type_id'] = $_product->getData('type_id');
                    }

                    //$quoteItem['categories'] = $this->_productCategories($_product);
                    //$prodUrl = $this->getProdUrl($_product, $storeUrl, $mage_store_view_id);
                    //$quoteItem['url_path'] = $prodUrl;

                    $quoteData['items'][$quoteItem['item_id']] = $quoteItem;
                }

                $quoteData = $this->_filter_output_data($quoteData, $this->response_mask['quote']);
                if (!empty($quoteData['items'])) {
                    $ret[] = $quoteData;
                }
            }

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function getProducts(
        $mage_store_id,
        $updated_at_min = null,
        $updated_at_max = null,
        $limit = null,
        $page = null,
        $handle = null,                // Not implemented
        $vendor = null,                // Not implemented
        $product_type = null,        // Not implemented
        $collection_id = null,        // Not implemented
        $since_id = null,
        $created_at_min = null,
        $created_at_max = null,
        $published_status = null,    // Not implemented
        $product_id = null)
    {

        register_shutdown_function('handleShutdown');
        $pageNumber = null;
        $pageSize = null;
        $ret = array();
        $myArgs = func_get_args();
        $this->simpleProductsStandalone = Mage::helper('mgconnector/configuration')->getValue('configurable_standalone', false);
        try {
            try {
                $store = Mage::app()->getStore($mage_store_id);
                if($store) {
                    //make sure current store not using flat catalog in this request (otherwise, disabled products wont be included)
                    $store->setConfig('catalog/frontend/flat_catalog_product', 0);
                    Mage::app()->setCurrentStore($store);
                }
            } catch (Exception $ex) {
                $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_ERROR, "Cannot set active store", $myArgs);
            }

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            $productCollectionWithPrices = Mage::getModel("catalog/product")
                ->getCollection()
                ->setStoreId($mage_store_id)
                ->applyFrontendPriceLimitations()
                ->addOrder('updated_at', 'ASC')
                ->addFinalPrice()
                ->addCategoryIds()
//	            ->addUrlRewrite()
                ->addAttributeToSelect('*');

            $productCollectionWithoutPrices = Mage::getModel("catalog/product")
                ->getCollection()
                ->setStoreId($mage_store_id)
//                ->applyFrontendPriceLimitations()
                ->addOrder('updated_at', 'ASC')
                ->addCategoryIds()
//	            ->addUrlRewrite()
                ->addAttributeToSelect('*');
            if ($updated_at_min != null) {
                $productCollectionWithPrices->addAttributeToFilter('updated_at', array('gt' => $updated_at_min));
                $productCollectionWithoutPrices->addAttributeToFilter('updated_at', array('gt' => $updated_at_min));
            }

            if ($updated_at_max != null) {
                $productCollectionWithPrices->addAttributeToFilter('updated_at', array('lt' => $updated_at_max));
                $productCollectionWithoutPrices->addAttributeToFilter('updated_at', array('lt' => $updated_at_max));
            }

            if ($since_id != null) {
                $productCollectionWithPrices->addAttributeToFilter('entity_id', array('gt' => $since_id));
                $productCollectionWithoutPrices->addAttributeToFilter('entity_id', array('gt' => $since_id));
            }

            if ($created_at_min != null) {
                $productCollectionWithPrices->addAttributeToFilter('created_at', array('gt' => $created_at_min));
                $productCollectionWithoutPrices->addAttributeToFilter('created_at', array('gt' => $created_at_min));
            }

            if ($created_at_max != null) {
                $productCollectionWithPrices->addAttributeToFilter('created_at', array('lt' => $created_at_max));
                $productCollectionWithoutPrices->addAttributeToFilter('created_at', array('lt' => $created_at_max));
            }

            if ($product_id != null) {
                $productCollectionWithPrices->addAttributeToFilter('entity_id', $product_id);
                $productCollectionWithoutPrices->addAttributeToFilter('entity_id', $product_id);
            }

            if ($limit != null) {
                $pageNumber = 1;        // Note that page numbers begin at 1
                $pageSize = $limit;
            }

            if ($page != null) {
                if (!is_null($pageSize)) {
                    $pageNumber = $page + 1;    // Note that page numbers begin at 1
                }
            }

            if (!is_null($pageSize)) {
                $productCollectionWithPrices->setPage($pageNumber, $pageSize);
                $productCollectionWithoutPrices->setPage($pageNumber, $pageSize);
            }

            $storeUrl = Mage::app()->getStore($mage_store_id)->getBaseUrl(Mage_Core_Model_Store::URL_TYPE_WEB, true);

            $productIds = array();
            $products = array();
            foreach ($productCollectionWithPrices as $product) {
                $productId = $product->getId();
                if (!in_array($productId, $productIds)) {
                    $productIds[] = $productId;
                    $products[] = $product;
                }
            }

            foreach ($productCollectionWithoutPrices as $product) {
                $productId = $product->getId();
                if (!in_array($productId, $productIds)) {
                    $productIds[] = $productId;
                    $products[] = $product;
                }
            }

            $this->loadProducts($mage_store_id, $productIds);

            $stockItemCollection = Mage::getModel('cataloginventory/stock_item')
                ->getCollection()
                ->addProductsFilter($productIds);
            $inventories = array();
            foreach ($stockItemCollection as $stockItem) {
                $productId = $stockItem->getProductId();
                $qty = $stockItem->getQty();
                $inventories[$productId] = $qty;
            }

            foreach ($products as $product) {
//                $product->setStoreId($mage_store_view_id)->load($product->getId());
                $productData = $product->toArray();
                $productId = $product->getId();
                $productData['base_image'] = $this->getImageUrl($product, 'image', $mage_store_id); //will bring base image (for backwards compatibility)
                $productData['small_image'] = $this->getImageUrl($product, 'small', $mage_store_id);
                $productData['thumbnail_image'] = $this->getImageUrl($product, 'thumbnail', $mage_store_id);

                $productData['categories'] = $this->_productCategories($product);
                $productData['price'] = $product->getFinalPrice();

                $prodUrl = $this->getProdUrl($product, $storeUrl, $mage_store_id);
                $productData['url_path'] = $prodUrl;
                $productData['name'] = $this->getProductName($product, $mage_store_id);
                $productData['is_salable'] = $product->isSalable();
                $productData['visibility'] = $product->getVisibility();
                if (key_exists($productId, $inventories))
                    $productData['inventory'] = $inventories[$productId];

                /*
                VISIBILITY_BOTH = 4
                VISIBILITY_IN_CATALOG = 2
                VISIBILITY_IN_SEARCH = 3
                VISIBILITY_NOT_VISIBLE = 1
                */

                $parent_id = $this->getProductParentId($product);
                if ($parent_id !== false) {
                    $productData['parent_id'] = $parent_id;
                }

                $productData = $this->_filter_output_data($productData, $this->response_mask['product']);
                $ret[] = $productData;
            }
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    private function getCategories($product)
    {
        $categories = array();
        $categoryCollection = $product->getCategoryCollection()->addAttributeToSelect('name');
        foreach ($categoryCollection as $category) {
            $categories[] = $category->getPath(); //getData('name');
        }
        return $categories;
    }

    public function getStoreSettings($mage_store_view_id = null)
    {
        register_shutdown_function('handleShutdown');
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);

            $currency = Mage::getStoreConfig('currency/options/default', $mage_store_view_id);
            $currencySymbol = Mage::app()->getLocale()->currency($currency)->getSymbol();
            $currencyFormat = Mage::getModel("directory/currency")->load($currency)->getOutputFormat();
            $storeName = Mage::getStoreConfig('general/store_information/name', $mage_store_view_id);

            $storeView = Mage::getModel('core/store');
            $storeView->load($mage_store_view_id);
            $storeGroup = $storeView->getGroup();
            $storeViewName = $storeView->name;
            $storeGroupName = $storeGroup->name;

            if (empty($storeName)) {
                $storeName = $storeGroupName;
            }
            $baseUrl = Mage::getStoreConfig('web/unsecure/base_url', $mage_store_view_id);
            $storeUrl = $storeView->getBaseUrl(Mage_Core_Model_Store::URL_TYPE_LINK);
            $ret = array(
                'domain' => $baseUrl,
                'storeFrontUrl' => $storeUrl,
                'name' => $storeName,
                'viewName' => $storeViewName,
                'phone' => Mage::getStoreConfig('general/store_information/phone', $mage_store_view_id),
                'contactEmail' => Mage::getStoreConfig('contacts/email/recipient_email', $mage_store_view_id),
                'timezone' => Mage::getStoreConfig('general/locale/timezone', $mage_store_view_id),
                'locale' => Mage::getStoreConfig('general/locale/code', $mage_store_view_id),
                'address' => Mage::getStoreConfig('general/store_information/address', $mage_store_view_id),
                'country' => Mage::getStoreConfig('general/store_information/merchant_country', $mage_store_view_id),
                'currency' => $currency,
                'money_format' => $currencySymbol,
                'money_with_currency_format' => $currencyFormat
            );

            $wsCollection = Mage::getModel("core/website")->getCollection();
            foreach ($wsCollection as $ws) {
                $websiteArr = $ws->toArray();
                $groups = $ws->getGroupCollection();
                foreach ($groups as $group) {
                    $groupArr = $group->toArray();
                    $stores = $group->getStoreCollection();
                    foreach ($stores as $store) {
                        $groupArr['views'][] = $store->toArray();
                    }
                    $websiteArr['groups'][] = $groupArr;
                }
                $ret['websites'][] = $websiteArr;
            }
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function getStoreOrderStatuses($mage_store_view_id)
    {
        register_shutdown_function('handleShutdown');
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            $statusesCollection = Mage::getModel('sales/order_status')->getCollection();
            $ret = array();
            foreach ($statusesCollection as $status) {
                $newStat = array();
                $newStat['status'] = $status->getData('status');
                $newStat['label'] = $status->getStoreLabel($mage_store_view_id);
                $ret[] = $newStat;
            }
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }


    public function createCoupon($rule_id, $coupon_code, $expiration_date = null)
    {
        register_shutdown_function('handleShutdown');
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            /**
             * @var $rule Mage_SalesRule_Model_Rule
             */
            $rule = Mage::getModel('salesrule/rule')->load($rule_id);

            $ruleId = empty($rule) ? null : $rule->getId();
            if (empty($ruleId)) {
                $msg = 'Given promotion ID does not exist';
                $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $msg, $myArgs);
                return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $msg);
            }

            if (!($rule->getUseAutoGeneration())) {
                $msg = 'Promotion not configured for multiple coupons generation';
                $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $msg, $myArgs);
                return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $msg);
            }

            $coupon = Mage::getModel('salesrule/coupon')
                ->setRule($rule)
                ->setIsPrimary(false)
                ->setCode($coupon_code)
                ->setAddedByRemarkety(1)
                ->setUsageLimit($rule->getUsesPerCoupon())
                ->setUsagePerCustomer($rule->getUsesPerCustomer())
                ->setType(Mage_SalesRule_Helper_Coupon::COUPON_TYPE_SPECIFIC_AUTOGENERATED);

            if ($expiration_date != null) {
                $coupon->setExpirationDate($expiration_date);
            } else {
                $coupon->setExpirationDate($rule->getToDate());
            }

            $coupon->save();

            $msg = "Successfuly created coupon code: " . $coupon_code . " for rule id: " . $rule_id;
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, $msg, $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function getCustomersCount($mage_store_view_id)
    {
        register_shutdown_function('handleShutdown');
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            $store_views = $this->_store_views_in_group($mage_store_view_id);
            $customers = Mage::getModel("customer/customer")->getCollection();
            $customers->addFieldToFilter('store_id', array('in' => $store_views));
            $count = $customers->getSize();
            $ret = array('count' => $count);

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function getOrdersCount($mage_store_view_id)
    {
        register_shutdown_function('handleShutdown');
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
//            $store_views = $this->_store_views_in_group($mage_store_view_id);
            $orders = Mage::getModel("sales/order")->getCollection();
            $orders->addFieldToFilter('store_id', array('eq' => $mage_store_view_id));
            $count = $orders->getSize();
            $ret = array('count' => $count);

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function getProductsCount($mage_view_id)
    {
        register_shutdown_function('handleShutdown');
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            $products = Mage::getModel("catalog/product")->getCollection();
            $products->addStoreFilter($mage_view_id);
            $count = $products->getSize();
            $ret = array('count' => $count);

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function getProductParentId($product)
    {
        if (!in_array($product->type_id, $this->_groupedTypes)) {
            $arrayOfParentIds = Mage::helper('mgconnector/links')->getParentId($product->getId());
            $parentId = null;

            if ($arrayOfParentIds && count($arrayOfParentIds)) {
                if (is_array($arrayOfParentIds)) {
                    $parentId = $arrayOfParentIds[0];
                } else {
                    $parentId = $arrayOfParentIds;
                }
            }

            if (!is_null($parentId)) {
                return $parentId;
            }
        }
        return false;
    }

    public function getProdUrl($product, $storeUrl, $mage_store_view_id)
    {
        $this->_debug(__FUNCTION__, "Start - prod id " . $product->getId(), null, '');
        $url = '';
        $visibility = $product->getVisibility();
        $parentProduct = null;
        if ($visibility == 1 || !in_array($product->type_id, $this->_groupedTypes)) {
            $parentId = null;
            if (!$this->simpleProductsStandalone) {
                $arrayOfParentIds = Mage::helper('mgconnector/links')->getParentId($product->getId());
                if ($arrayOfParentIds && count($arrayOfParentIds)) {
                    if (is_array($arrayOfParentIds)) {
                        $parentId = $arrayOfParentIds[0];
                    } else {
                        $parentId = $arrayOfParentIds;
                    }
                }
            }
            if (!is_null($parentId)) {
                $this->_debug(__FUNCTION__, "parent id " . $parentId, null, '');
                $product_id = $parentId;
                $parentProduct = $this->loadProduct($parentId);
            } else {
                $product_id = $product->getId();
            }
        } else {
            $product_id = $product->getId();
        }


        try {
            $useInternalMethods = Mage::helper('mgconnector/configuration')->getProductUrlFromMagento();
            if(!$useInternalMethods){
                $url = Mage::helper('mgconnector/urls')->getValue($product_id, $mage_store_view_id);
                if (!empty($url)) {
                    $url = $storeUrl . $url;
                    if (Mage::getStoreConfig('catalog/seo/product_url_suffix', $mage_store_view_id)) {
                        $url .= Mage::getStoreConfig('catalog/seo/product_url_suffix', $mage_store_view_id);
                    }
                }
                $this->_debug(__FUNCTION__, "after setStoreId getProductUrl: " . $url, null, '');
            }
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, "failed after setStoreId: ", $e->getMessage(), '');
        }

        if(empty($url) && !empty($parentProduct)){
            $url = $parentProduct->getProductUrl();
        }

        if(empty($url) && !empty($product)){
            $url = $product->getProductUrl();
        }

        $this->_debug(__FUNCTION__, "getProductUrl: " . $url, null, '');

        return $url;

    }

    public function getImageUrl($product, $type = 'image', $mage_store_view_id = null)
    {

        $url = '';
        if (!$this->simpleProductsStandalone && !in_array($product->type_id, $this->_groupedTypes)) {
            $this->_debug(__FUNCTION__, null, "not config prod id" . $product->getId(), '');
            $arrayOfParentIds = Mage::helper('mgconnector/links')->getParentId($product->getId());
            $parentId = null;

            if ($arrayOfParentIds && count($arrayOfParentIds)) {
                if (is_array($arrayOfParentIds)) {
                    $parentId = $arrayOfParentIds[0];
                } else {
                    $parentId = $arrayOfParentIds;
                }
            }

            if (!is_null($parentId)) {
                $this->_debug(__FUNCTION__, null, "parent id: " . $parentId, '');
                $product = $this->loadProduct($parentId);
            }
        } else {
            $this->_debug(__FUNCTION__, null, "configurable prod id" . $product->getId(), '');
        }
        $this->_debug(__FUNCTION__, null, "Getting image url for product: " . $product->getId(), '');

        switch ($type) {
            case "small":
                $imagePath = $product->getSmallImage();
                break;
            case "thumbnail":
                $imagePath = $product->getThumbnail();
                break;
            default:
                $imagePath = $product->getImage();
        }

        $url = (string)Mage::getBaseUrl(Mage_Core_Model_Store::URL_TYPE_MEDIA) . 'catalog/product' . $imagePath;
        $this->_debug(__FUNCTION__, null, $type . " url: " . $url, '');
        return $url;

    }

    public function getCartRecoveryURL($cart_id, $storeId = null)
    {
        $recovery = Mage::getModel('mgconnector/recovery');
        $id = $recovery->encodeQuoteId($cart_id);
        if($storeId != null) {
            $url = (string)Mage::app()->getStore($storeId)->getBaseUrl(Mage_Core_Model_Store::URL_TYPE_LINK) . 'remarkety/recovery/cart/quote_id/' . $id;
        } else {
            $url = (string)Mage::getBaseUrl(Mage_Core_Model_Store::URL_TYPE_LINK) . 'remarkety/recovery/cart/quote_id/' . $id;
        }

        return $url;
    }

    public function getProductName($product, $mage_store_view_id)
    {
        $name = '';
        if (!$this->simpleProductsStandalone && !in_array($product->type_id, $this->_groupedTypes)) {
            $this->_debug(__FUNCTION__, null, "not config prod id " . $product->getId(), '');
            $arrayOfParentIds = Mage::helper('mgconnector/links')->getParentId($product->getId());
            $parentId = null;

            if ($arrayOfParentIds && count($arrayOfParentIds)) {
                if (is_array($arrayOfParentIds)) {
                    $parentId = $arrayOfParentIds[0];
                } else {
                    $parentId = $arrayOfParentIds;
                }
            }

            if (!is_null($parentId)) {
                $this->_debug(__FUNCTION__, null, "parent id: " . $parentId, '');
                $product = $this->loadProduct($parentId);
//    			$product->setStoreId($mage_store_view_id)->load($parentId);
                //$product->setStoreId($mage_store_view_id);
            }
        } else {
            $this->_debug(__FUNCTION__, null, "configurable prod id " . $product->getId(), '');
        }

        $name = $product->getName();
        $this->_debug(__FUNCTION__, null, "Prod name: " . $name, '');
        return $name;
    }

    public function unsubscribe($email)
    {
        register_shutdown_function('handleShutdown');
        $myArgs = func_get_args();

        try {
            if (empty($email)) {
                $msg = 'No email sent passed to unsubsribe';
                $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $msg, $myArgs);
                return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $msg);
            }

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            $subscriber = Mage::getModel('newsletter/subscriber')->loadByEmail($email);

            $data = $subscriber->getData();
            if (empty($data)) {
                $msg = 'Given subscriber does not exist';
                $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $msg, $myArgs);
                return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $msg);
            }
            Mage::register('remarkety_subscriber_deleted', true, true);
            $subscriber->unsubscribe();

            $msg = "Successfuly unsubscribed customer using email: " . $email;
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, $msg, $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Mage_Core_Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    private function getConfigProdModel()
    {
        if ($this->configurable_product_model == null) {
            $this->configurable_product_model = Mage::getModel('catalog/product_type_configurable');
        }
        return $this->configurable_product_model;
    }

    public function loadProduct($productId)
    {
        if (!isset($this->_productCache[$productId])) {
            $this->_productCache[$productId] = Mage::getModel("catalog/product")->load($productId);
        }
        return $this->_productCache[$productId];
    }

    /**
     * Returns a list of payment methods used in each order
     * @param $orderIds
     * @return array
     */
    private function loadPaymentMethods($orderIds)
    {

        $orderIds = array_unique($orderIds);
        $paymentsCollection = Mage::getModel("sales/order_payment")
            ->getCollection()
            ->addOrder('main_table.entity_id', 'DESC')
            ->addAttributeToFilter('main_table.parent_id', array('in' => $orderIds))
            ->addAttributeToSelect('parent_id')
            ->addAttributeToSelect('method');

        $payments = array();
        $paymentMethodsNames = array();

        foreach ($paymentsCollection as $payment) {
            $row = $payment->toArray();
            if (!isset($payments[$row['parent_id']])) {
                $method = $row['method'];
                //convert method code to name
                if (!isset($paymentMethodsNames[$method])) {
                    $methodName = Mage::getStoreConfig('payment/' . $method . '/title');
                    if (!empty($methodName)) {
                        $paymentMethodsNames[$method] = $methodName;
                    } else {
                        $paymentMethodsNames[$method] = $method;
                    }
                }
                //save to list with order id as key
                $payments[$row['parent_id']] = $paymentMethodsNames[$method];
            }
        }

        return $payments;
    }

    private function loadProducts($storeId, $productIds, $loadParents = true)
    {
        if (empty($productIds))
            return;
        $productIds = array_unique($productIds);
        $productsCollection = Mage::getModel("catalog/product")
            ->getCollection()
            ->setStoreId($storeId)
            ->addFieldToFilter('entity_id', array($productIds))
            ->applyFrontendPriceLimitations()
            ->addOrder('updated_at', 'ASC')
            ->addFinalPrice()
            ->addCategoryIds()
//	            ->addUrlRewrite()
            ->addAttributeToSelect('*');
        $productsCollection->load(false, true);
        $parentIds = array();
        foreach ($productsCollection as $product) {
            $productId = $product->getId();
            if (!array_key_exists($productId, $this->_productCache)) {
                $this->_productCache[$productId] = $product; //Mage::getModel("catalog/product")->load($productId);
                if ($loadParents)
                    $parentIds[] = $this->getProductParentId($product);
            }
        }
        if ($loadParents)
            $this->loadProducts($storeId, $parentIds, false);
    }

    public function sendLogInResponse()
    {
        $this->_sendLogInResponse = true;
    }

    private function _setupConfiguration()
    {
        $keys = array('categories-to-ignore', 'configurable_standalone');

        $this->simpleProductsStandalone = Mage::getStoreConfig('remarkety/mgconnector/configurable_standalone');
    }

    public function getQueueSize($mage_store_view_id)
    {
        register_shutdown_function('handleShutdown');
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
//            $store_views = $this->_store_views_in_group($mage_store_view_id);
            $queue = Mage::getModel('mgconnector/queue')->getCollection();
            $queue->addFieldToFilter('store_id', array('eq' => $mage_store_view_id));
            $count = $queue->getSize();
            $ret = array('count' => $count);

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse($ret, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function getQueueItems($mage_store_view_id, $limit = null, $page = null, $minId = null, $maxId = null)
    {
        try {
            $myArgs = func_get_args();

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);

            $collection = Mage::getModel('mgconnector/queue')->getCollection();
            $collection->addFieldToFilter('store_id', array('eq' => $mage_store_view_id));

            if(!empty($minId)){
                $collection->addFieldToFilter('queue_id', array('gteq' => $minId));
            }

            if(!empty($maxId)){
                $collection->addFieldToFilter('queue_id', array('lteq' => $maxId));
            }

            $pageSize = null;
            $pageNumber = 1;        // Note that page numbers begin at 1 and remarkety starts at 0
            if ($limit != null) {
                $pageSize = $limit;
            }

            if ($page != null) {
                if (!is_null($pageSize)) {
                    $pageNumber = $page + 1;    // Note that page numbers begin at 1 and remarkety starts at 0
                }
            }

            if (!is_null($pageSize)) {
                $collection->getSelect()->limit($pageSize, ($pageNumber-1)*$pageSize);
            }
            $data = array();
            foreach ($collection as $queueEvent) {
                $event = $queueEvent->toArray();
                $event['payload'] = json_encode(unserialize($event['payload']));
                $data[] = $event;
            }
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, $myArgs);
            return $this->_wrapResponse($data, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function deleteQueueItems($mage_store_view_id, $minId = null, $maxId = null){
        try {
            $myArgs = func_get_args();

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);

            $collection = Mage::getModel('mgconnector/queue')->getCollection();
            $collection->addFieldToFilter('store_id', array('eq' => $mage_store_view_id));

            if(!empty($minId)){
                $collection->addFieldToFilter('queue_id', array('gteq' => $minId));
            }

            if(!empty($maxId)){
                $collection->addFieldToFilter('queue_id', array('lteq' => $maxId));
            }

            $toDelete = $collection->count();
            $itemsDeleted = 0;

            foreach ($collection as $item) {
                $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, "inside delete events loop", array('id' => $item->getId()));
                $item->delete();
                $itemsDeleted++;
            }

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, $myArgs);

            return $this->_wrapResponse(array(
                'totalMatching' => $toDelete,
                'totalDeleted' => $itemsDeleted
            ), REMARKETY_MGCONNECTOR_SUCCESS_STATUS);

        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function retryQueueItems($mage_store_view_id, $limit = null, $page = null, $minId = null, $maxId = null)
    {
        try {
            $myArgs = func_get_args();

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);

            $collection = Mage::getModel('mgconnector/queue')->getCollection();
            $collection->addFieldToFilter('store_id', array('eq' => $mage_store_view_id));

            if(!empty($minId)){
                $collection->addFieldToFilter('queue_id', array('gteq' => $minId));
            }

            if(!empty($maxId)){
                $collection->addFieldToFilter('queue_id', array('lteq' => $maxId));
            }

            $pageSize = null;
            $pageNumber = 1;        // Note that page numbers begin at 1 and remarkety starts at 0
            if ($limit != null) {
                $pageSize = $limit;
            }

            if ($page != null) {
                if (!is_null($pageSize)) {
                    $pageNumber = $page + 1;    // Note that page numbers begin at 1 and remarkety starts at 0
                }
            }

            if (!is_null($pageSize)) {
                $collection->getSelect()->limit($pageSize, ($pageNumber-1)*$pageSize);
            }

            $observer = Mage::getModel('mgconnector/observer');
            $itemsSent = $observer->resend($collection);

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, $myArgs);
            return $this->_wrapResponse(array(
                'totalMatching' => $collection->count(),
                'sentSuccessfully' => $itemsSent
            ), REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function getConfig($mage_store_id, $configName, $scope)
    {
        register_shutdown_function('handleShutdown');
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            $configPath = 'remarkety/mgconnector/' . $configName;
            $value = null;
            switch($scope){
                case 'stores':
                    $store = Mage::app()->getStore($mage_store_id);
                    if($store) {
                        $value = $store->getConfig($configPath);
                    }
                    break;
                default:
                    $value = Mage::getStoreConfig($configPath);
            }

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse(Array(
                'value' => $value
            ), REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }

    public function setConfig($mage_store_id, $configName, $scope, $newValue)
    {
        register_shutdown_function('handleShutdown');
        $myArgs = func_get_args();

        try {
            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_CALLED_STATUS, null, $myArgs);
            $configPath = 'remarkety/mgconnector/' . $configName;
            $value = null;
            if($scope !== 'stores'){
                $scope = 'default';
                $mage_store_id = 0;
            }
            Mage::getModel('core/config')->saveConfig(
                $configPath,
                $newValue,
                $scope,
                $mage_store_id
            );

            Mage::app()->getCacheInstance()->cleanType('config');
            Mage::dispatchEvent('adminhtml_cache_refresh_type', array('type' => 'config'));

            $this->_debug(__FUNCTION__, REMARKETY_MGCONNECTOR_SUCCESS_STATUS, null, '');
            return $this->_wrapResponse(true, REMARKETY_MGCONNECTOR_SUCCESS_STATUS);
        } catch (Exception $e) {
            $this->_log(__FUNCTION__, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage(), $myArgs);
            return $this->_wrapResponse(null, REMARKETY_MGCONNECTOR_FAILED_STATUS, $e->getMessage());
        }
    }
}


function handleShutdown()
{
    $error = error_get_last();
    if ($error !== NULL) {
        $error_info = "[SHUTDOWN] file:" . $error['file'] . " | ln:" . $error['line'] . " | msg:" . $error['message'] . PHP_EOL;
        Mage::log($error_info, null, REMARKETY_LOG, true);
    } else {
        Mage::log("SHUTDOWN", null, REMARKETY_LOG);
    }
}
