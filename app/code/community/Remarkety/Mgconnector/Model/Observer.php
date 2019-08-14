<?php

/**
 * Observer model, which handle few events and send post request
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */

if (!defined("REMARKETY_LOG"))
    define('REMARKETY_LOG', 'remarkety_mgconnector.log');

class Remarkety_Mgconnector_Model_Observer
{
    const REMARKETY_EVENTS_ENDPOINT = 'https://webhooks.remarkety.com/webhooks';
    const REMARKETY_METHOD = 'POST';
    const REMARKETY_TIMEOUT = 2;
    const REMARKETY_VERSION = 0.9;
    const REMARKETY_PLATFORM = 'MAGENTO';

    const EVENT_ORDERS_CREATED = 'orders/create';
    const EVENT_ORDERS_UPDATED = 'orders/updated';
    const EVENT_ORDERS_DELETE = 'orders/delete';
    const EVENT_CUSTOMERS_CREATED = 'customers/create';
    const EVENT_CUSTOMERS_UPDATED = 'customers/update';
    const EVENT_CUSTOMERS_DELETE = 'customers/delete';
    const EVENT_PRODUCTS_CREATED = 'products/create';
    const EVENT_PRODUCTS_UPDATED = 'products/update';
    const EVENT_PRODUCTS_DELETED = 'products/delete';

    protected $_token = null;
    protected $_intervals = null;
    protected $_customer = null;
    protected $_hasDataChanged = false;

    protected $_subscriber = null;
    protected $_origSubsciberData = null;

    protected $_address = null;
    protected $_origAddressData = null;

    protected $_sentEventsHash = array();
    protected $_productStoreIds;

    private $response_mask = array(
        'product' => array(
            'id',
            'sku',
            'title',
            'created_at',
            'updated_at',
            'type_id',
            'base_image',
            'thumbnail_image',
            'enabled',
            'visibility',
            'categories',
            'small_image',
            'price',
            'special_price',
            'cost',
            'url',
            'is_in_stock',
            'parent_id'
        )
    );

    /**
     * @var $_configHelp Remarkety_Mgconnector_Helper_Configuration
     */
    private $_configHelp;

    /**
     * @var $rmCore Remarkety_Mgconnector_Model_Core
     */
    private $_rmCore;

    private $_forceAsync = false;

    public function __construct()
    {
        $this->_token = Mage::getStoreConfig('remarkety/mgconnector/api_key');
        $intervals = Mage::getStoreConfig('remarkety/mgconnector/intervals');
        if(empty($intervals)){
            $intervals = "1,3,10";
        }
        $this->_intervals = explode(',', $intervals);
        $this->_configHelp = Mage::helper('mgconnector/configuration');
        $this->_rmCore = Mage::getModel("mgconnector/core");

        $this->_forceAsync = Mage::helper('mgconnector/configuration')->getValue('forceasyncwebhooks', false);
    }

    private function _log($function, $status, $message, $data, $logLevel = Zend_Log::DEBUG)
    {
        $logMsg = implode(REMARKETY_LOG_SEPARATOR, array($function, $status, $message, json_encode($data)));
//        $force = ($status != REMARKETY_MGCONNECTOR_CALLED_STATUS);
        Mage::log($logMsg, $logLevel, REMARKETY_LOG);
    }

    public function triggerOrderUpdate($observer){
        $order = $observer->getOrder();
        if(!$this->_configHelp->isWebhooksEnabled($order->getStore()->getId())){
            return $this;
        }

        $eventType = self::EVENT_ORDERS_UPDATED;
        $originalData = $order->getOrigData();
        if ($originalData === null) {
            $eventType = self::EVENT_ORDERS_CREATED;
        } else {
            if(!empty($originalData['status']) && $originalData['status'] == $order->getStatus()){
                //Status haven't changed, should ignore this event
                if(!Mage::helper('mgconnector/configuration')->forceOrderUpdates()){
                    $this->_log(__FUNCTION__, '', 'Order ('.$order->getIncrementId().') status was not changed', null);
                    return $this;
                }
            }
        }

        $data = $this->serializeOrder($order);
        $this->makeRequest($eventType, $data, $order->getStore()->getId());
    }

    /**
     * @param array $address
     * @return array
     */
    private function serializeAddress($address){
        $country = false;
        if(!empty($address['country_id'])){
            $country = Mage::getModel('directory/country')->loadByCode($address['country_id']);
        }
        return array(
            'first_name' => $address['firstname'],
            'last_name' => $address['lastname'],
            'city' => $address['city'],
            'street' => $address['street'],
            'country_code' => $address['country_id'],
            'country' => $country ? $country->getName() : null,
            'zip' => $address['postcode'],
            'phone' => $address['telephone'],
            'region' => $address['region'],
            'company' => !empty($address['company']) ? $address['company'] : null
        );
    }

    /**
     * @param Mage_Customer_Model_Customer $customer
     * @return array
     */
    public function serializeCustomer($customer){

        $groupModel = Mage::getModel("customer/group");
        $groups = array();
        $group_id = $customer->getGroupId();
        if(!empty($group_id)) {
            $groupModel->load($customer->getGroupId());
            $groupName = $groupModel->getCustomerGroupCode();
            $groups[] = array(
                'id' => $customer->getGroupId(),
                'name' => $groupName
            );
        }

        $subscriberModel = Mage::getModel("newsletter/subscriber");
        $subscriberModel->loadByEmail($customer->getEmail());

        $billingAddress = $customer->getDefaultBillingAddress();
        $customerAddress = $billingAddress ? $billingAddress : $customer->getDefaultShippingAddress();

        $tags = $this->_getCustomerProductTags($customer);
        $tagsArr = array();
        if (!empty($tags) && $tags->getSize()) {
            foreach ($tags as $_tag) {
                $tagsArr[] = $_tag->getName();
            }
        }

        if($customer->getIsSubscribed()){
            $allowed = true;
        } else {
            $allowed = $subscriberModel->isSubscribed();
        }

        $gender = $customer->getResource()->getAttribute("gender");
        $genderVal = null;
        if ($gender->usesSource()) {
            $genderVal = $gender->getSource()->getOptionText($customer->getGender());
        }

        $info = array(
            'id' => $customer->getId(),
            'storeId' => $customer->getStore()->getId(),
            'accepts_marketing' => $allowed,
            'birthdate' => $customer->getDob(),
            'email' => $customer->getEmail(),
            'title' => $customer->getPrefix(),
            'first_name' => $customer->getFirstname(),
            'last_name' => $customer->getLastname(),
            'groups' => $groups,
            'created_at' => $customer->getCreatedAt(),
            'updated_at' => $customer->getUpdatedAt(),
            'guest' => false,
            'default_address' => $customerAddress ? $this->serializeAddress($customerAddress->getData()) : null,
            'tags' => $tagsArr,
            'gender' => !empty($genderVal) ? $genderVal : null
        );
        $extensionHelper = Mage::helper('mgconnector/extension');
        $rewardPointsInstance = $extensionHelper
            ->getRewardPointsIntegrationInstance();
        if ($rewardPointsInstance !== false) {
            $info['rewards'] = $rewardPointsInstance
                ->getCustomerUpdateData($customer->getId());
        }

        return $info;
    }

    /**
     * @param $orderId
     * @return string|null
     */
    private function loadPaymentMethod($orderId)
    {

        $paymentsCollection = Mage::getModel("sales/order_payment")
            ->getCollection()
            ->addOrder('main_table.entity_id', 'DESC')
            ->addAttributeToFilter('main_table.parent_id', array('eq' => $orderId))
            ->addAttributeToSelect('parent_id')
            ->addAttributeToSelect('method');


        foreach ($paymentsCollection as $payment) {
            $row = $payment->toArray();
            $method = $row['method'];
            $methodName = Mage::getStoreConfig('payment/' . $method . '/title');
            if (!empty($methodName)) {
                return $methodName;
            } else {
                return $method;
            }
        }

        return null;
    }

    /**
     * @param Mage_Sales_Model_Order $order
     * @return bool|array
     */
    public function serializeOrder($order){

        $orderData = $order->toArray();
        if (!empty($orderData['relation_child_id'])) {
            return false;
        }


        $shippingAddress = null;
        if (isset($orderData['shipping_address_id']) && $orderData['shipping_address_id']) {
            $shippingAddress = Mage::getModel('sales/order_address')->load($orderData['shipping_address_id'])->toArray();
        }
        $billingAddress = null;
        if (isset($orderData['billing_address_id']) && $orderData['billing_address_id']) {
            $billingAddress = Mage::getModel('sales/order_address')->load($orderData['billing_address_id'])->toArray();
        }

        /**
         * @var $rmCore Remarkety_Mgconnector_Model_Core
         */
        $rmCore = Mage::getModel("mgconnector/core");

        $customerAddress = null;
        if($orderData['customer_is_guest']) {
            $subscriberModel = Mage::getModel("newsletter/subscriber");
            $subscriberModel->loadByEmail($orderData['customer_email']);

            $customerAddress = !empty($billingAddress) ? $billingAddress : $shippingAddress;
            $customerInfo = array(
                'id' => $orderData['customer_id'],
                'accepts_marketing' => $subscriberModel->isSubscribed(),
                'birthdate' => $orderData['customer_dob'],
                'email' => $orderData['customer_email'],
                'title' => $orderData['customer_prefix'],
                'first_name' => $orderData['customer_firstname'],
                'last_name' => $orderData['customer_lastname'],
                'groups' => array(),
                'created_at' => $orderData['created_at'],
                'updated_at' => $orderData['updated_at'],
                'guest' => true,
                'default_address' => $this->serializeAddress($customerAddress)
            );
        } else {
            //get address for customer
            $customer_id = $order->getCustomerId();
            $customer = Mage::getModel('customer/customer')->load($customer_id);
            $customerInfo = $this->serializeCustomer($customer);
        }

        $discounts = array();
        if(!empty($orderData['coupon_code'])){
            $discounts[] = array(
                'code' => $orderData['coupon_code'],
                'amount' => (float)$orderData['discount_amount']
            );
        }

        $line_items = array();
        $itemsCollection = $order->getAllVisibleItems();
        $store = $order->getStore();
        $storeUrl = $store->getBaseUrl(Mage_Core_Model_Store::URL_TYPE_WEB, true);
        $store_id = $store->getId();

        /**
         * @var $item Mage_Sales_Model_Order_Item
         */
        foreach ($itemsCollection as $item) {
            try {
                $enabled = $item->getProduct()->getIsSalable();
                if($enabled){
                    $enabled = $item->getProduct()->isVisibleInCatalog() || $item->getProduct()->isVisibleInSiteVisibility();
                }

                $images = $this->getProductImages($item->getProduct());

                $itemArr = array(
                    'product_parent_id' => $rmCore->getProductParentId($item->getProduct()),
                    'product_id' => $item->getProductId(),
                    'quantity' => (float)$item->getQtyOrdered(),
                    'sku' => $item->getSku(),
                    'name' => $item->getName(),
                    'title' => $rmCore->getProductName($item->getProduct(), $store_id),
                    'price' => (float)$item->getPrice(),
                    'price_base' => (float)$item->getBasePrice(),
                    'product_exists' => $enabled,
                    'url' => $rmCore->getProdUrl($item->getProduct(), $storeUrl, $store_id),
                    'images' => $images,
                    'tax_amount' => (float)$item->getTaxAmount(),
                    'discount' => (float)$item->getDiscountAmount()
                );

                $line_items[] = $itemArr;
            } catch (Exception $e) {
                $this->_log(__FUNCTION__, 'Error in handling order items for order ID ' . $orderData['entity_id'], $e->getMessage(), $orderData, Zend_Log::ERR);
            }
        }

        $shipping_lines = array();
        $shipmentCollection = Mage::getResourceModel('sales/order_shipment_collection')
            ->setOrderFilter($order)
            ->load();
        /**
         * @var $shipment Mage_Sales_Model_Order_Shipment
         */
        foreach ($shipmentCollection as $shipment){
            /**
             * @var $tracknum Mage_Sales_Model_Order_Shipment_Track
             */
            foreach($shipment->getAllTracks() as $tracknum)
            {
                $shipping_lines[] = array(
                    'title' => $tracknum->getTitle(),
                    'tracking_number' => $tracknum->getNumber()
                );
            }

        }

        $data = array(
            'storeId' => $store->getId(),
            'id' => (empty($orderData['original_increment_id'])) ? $orderData['increment_id'] : $orderData['original_increment_id'],
            'name' => $orderData['increment_id'],
            'created_at' => $orderData['created_at'],
            'updated_at' => $orderData['updated_at'],
            'currency' => $orderData['order_currency_code'],
            'discount_codes' => $discounts,
            'email' => $orderData['customer_email'],
            'payment_method' => $this->loadPaymentMethod($order->getId()),
            'fulfillment_status' => '',
            'line_items' => $line_items,
            'note' => $orderData['customer_note'],
            'status' => array(
                'code' => $order->getStatus(),
                'name' => $order->getStatusLabel(),
            ),
            'subtotal_price' => $orderData['subtotal'],
            'taxes_included' => true,
            'total_discounts' => $orderData['discount_amount'],
            'total_price' => $orderData['grand_total'],
            'total_shipping' => $orderData['shipping_amount'],
            'total_tax' => $orderData['tax_amount'],
            'customer' => $customerInfo,
            'shipping_address' => $this->serializeAddress($shippingAddress),
            'billing_address' => $this->serializeAddress($billingAddress),
            'shipping_lines' => $shipping_lines
        );
        return $data;
    }

    private function getProductImages($product){
        /**
         * @var $rmCore Remarkety_Mgconnector_Model_Core
         */
        $rmCore = Mage::getModel("mgconnector/core");

        $images = array();
        $baseImage = $rmCore->getImageUrl($product, 'image');
        if(!empty($baseImage)){
            $images[] = array(
                'src' => $baseImage,
                'type' => 'base_image'
            );
        }
        $smallImage = $rmCore->getImageUrl($product, 'small');
        if(!empty($smallImage)){
            $images[] = array(
                'src' => $smallImage,
                'type' => 'small_image'
            );
        }
        $thumbnailImage = $rmCore->getImageUrl($product, 'thumbnail');
        if(!empty($thumbnailImage)){
            $images[] = array(
                'src' => $thumbnailImage,
                'type' => 'thumbnail_image'
            );
        }
        return $images;
    }

    /**
     * @param Mage_Catalog_Model_Product $product
     * @param $storeId
     * @return array
     */
    public function serializeProduct($product, $storeId){

        /**
         * @var $rmCore Remarkety_Mgconnector_Model_Core
         */
        $rmCore = Mage::getModel("mgconnector/core");

        $store = Mage::App()->getStore($storeId);
        $storeUrl = $store->getBaseUrl(Mage_Core_Model_Store::URL_TYPE_WEB, true);

        $categories = $rmCore->_productCategories($product);
        $categories = array_map(function ($val) {
            return array(
                'name' => $val
            );
        }, $categories);

        $now = Mage::getSingleton('core/date')->timestamp(time());
        $rules = Mage::getResourceModel('catalogrule/rule');
        $price = $rules->getRulePrice($now, $store->getWebsiteId(), 0, $product->getId());
        $price = empty($price) ? $product->getFinalPrice() : (float)$price;
        $special_price = $product->getSpecialPrice();

        $stocklevel = (int)Mage::getModel('cataloginventory/stock_item')
            ->loadByProduct($product)->getQty();

        $originalStore = Mage::app()->getStore();
        Mage::app()->setCurrentStore($storeId);
        $product = $rmCore->loadProduct($product->getId());
        $stores = $product->getStoreIds();
        if(!in_array($storeId, $stores)){
            $enabled = false;
        } else {
            $enabled = $product->isAvailable() && ($product->isVisibleInCatalog() || $product->isVisibleInSiteVisibility());
        }
        $url = $rmCore->getProdUrl($product, $storeUrl, $storeId);
        Mage::app()->setCurrentStore($originalStore->getId());

        $data = array(
            'id' => $product->getId(),
            'sku' => $product->getSku(),
            'title' => $rmCore->getProductName($product, $storeId),
            'body_html' => '',
            'categories' => $categories,
            'created_at' => $product->getCreatedAt(),
            'updated_at' => $product->getUpdatedAt(),
            'images' => $this->getProductImages($product),
            'enabled' => $enabled,
            'price' => $price,
            'special_price' => $special_price,
            'url' => $url,
            'parent_id' => $rmCore->getProductParentId($product),
            'variants' => array(
                array(
                    'inventory_quantity' => $stocklevel,
                    'price' => $price
                )
            )
        );

        return $data;
    }

    public function triggerCustomerAddressBeforeUpdate($observer)
    {
        $address = Mage::getSingleton('customer/session')
            ->getCustomer()
            ->getDefaultBillingAddress();

        if (!empty($address)) {
            $this->_origAddressData = $address->getData();
        }

        return $this;
    }

    public function beforeBlockToHtml(Varien_Event_Observer $observer)
    {
        $grid = $observer->getBlock();

        /**
         * Mage_Adminhtml_Block_Customer_Grid
         */
        if ($grid instanceof Mage_Adminhtml_Block_Promo_Quote_Edit_Tab_Coupons_Grid) {
            $grid->addColumnAfter(
                'expiration_date',
                array(
                    'header' => Mage::helper('salesrule')->__('Expiration date'),
                    'index' => 'expiration_date',
                    'type'   => 'datetime',
                    'default'   => '-',
                    'align'  => 'center',
                    'width'  => '160'
                ),
                'created_at'
            );

            $yesnoOptions = array(null => 'No', '1' => 'Yes', '' => 'No');

            $grid->addColumnAfter(
                'added_by_remarkety',
                array(
                    'header' => Mage::helper('salesrule')->__('Created By Remarkety'),
                    'index' => 'added_by_remarkety',
                    'type' => 'options',
                    'options' => $yesnoOptions,
                    'width' => '30',
                    'align' => 'center',
                ),
                'expiration_date'
            );
        }

    }

    public function triggerCustomerAddressUpdate($observer)
    {
        $this->_address = $observer->getEvent()->getCustomerAddress();
        $this->_customer = $this->_address->getCustomer();

        if(!$this->_configHelp->isStoreInstalled($this->_customer->getStore()->getId())){
            return $this;
        }

        if (Mage::registry('remarkety_customer_save_observer_executed_' . $this->_customer->getId())) {
            return $this;
        }

        $isDefaultBilling =
            ($this->_customer == null || $this->_customer->getDefaultBillingAddress() == null)
                ? false
                : ($this->_address->getId() == $this->_customer->getDefaultBillingAddress()->getId());
        if (!$isDefaultBilling || !$this->_customer->getId()) {
            return $this;
        }

        if($this->_customerUpdate()){
            Mage::register(
                'remarkety_customer_save_observer_executed_' . $this->_customer->getId(),
                true
            );
        }


        return $this;
    }

    private function shouldUpdateRule($rule){
        if(!$this->shouldSendProductUpdates()){
            return false;
        }
        $now = new DateTime();
        $currentFromDate = new DateTime($rule->getFromDate());
        $currentToDate = new DateTime($rule->getToDate());
        $now->setTime(0, 0, 0);
        $currentFromDate->setTime(0, 0, 0);
        $currentToDate->setTime(0, 0, 0);
        if($currentFromDate <= $now && $currentToDate >= $now && $rule->getIsActive()){
            $oldData = $rule->getOrigData();
            if(!is_null($oldData) && isset($oldData['is_active']) && $oldData['is_active'] == 1){
                //check if was already active so no need to update
                $oldFromDate = new DateTime($oldData['from_date']);
                $oldToDate = new DateTime($oldData['to_date']);
                $oldFromDate->setTime(0, 0, 0);
                $oldToDate->setTime(0, 0, 0);
                if($rule->hasDataChanges()) {
                    return true;
                }
                if($currentFromDate <= $now && $currentToDate >= $now){
                    return false;
                }
            }
            return true;
        }
        //check if was already active but not active now so need to update
        $oldData = $rule->getOrigData();
        if(!is_null($oldData) && isset($oldData['is_active']) && $oldData['is_active'] == 1){
            $currentFromDate = new DateTime($oldData['from_date']);
            $currentToDate = new DateTime($oldData['to_date']);
            $currentFromDate->setTime(0, 0, 0);
            $currentToDate->setTime(0, 0, 0);
            if($currentFromDate <= $now && $currentToDate >= $now){
                return true;
            }
        }
        return false;
    }
    public function triggerCatalogRuleBeforeUpdate($observer)
    {

        $this->rule = $observer->getEvent()->getRule();
        $this->rule->setUpdatedAt(date("Y-m-d H:i:s"));
        if($this->shouldUpdateRule($this->rule)) {
            $this->_queueRequest('catalogruleupdated', array('ruleId' => $this->rule->getId()), 1, null);
            //$this->sendProductPrices($this->rule->getId());
        }
    }

    public function triggerCatalogRuleBeforeDelete($observer){
    }

    public function triggerCatalogRuleAfterDelete($observer){
    }

    public function triggerCustomerUpdate($observer)
    {
        $this->_customer = $observer->getEvent()->getCustomer();

        if(!$this->_configHelp->isStoreInstalled($this->_customer->getStore()->getId())){
            return $this;
        }

        if (Mage::registry('remarkety_customer_save_observer_executed_' . $this->_customer->getId()) || !$this->_customer->getId()) {
            return $this;
        }

        if ($this->_customer->getOrigData() === null) {
            $this->_customerRegistration();
            $done = true;
        } else {
            $done = $this->_customerUpdate();
        }

        if($done) {
            Mage::register(
                'remarkety_customer_save_observer_executed_' . $this->_customer->getId(),
                true
            );
        }

        return $this;
    }

    public function triggerCartUpdated($observer){
        try {
            $cart = $observer->getCart();
            if($cart){
                $quote = $cart->getQuote();
                $email = Mage::getSingleton('customer/session')->getSubscriberEmail();
                if ($email && $quote && !is_null($quote->getId()) && is_null($quote->getCustomerEmail())) {
                    $quote->setCustomerEmail($email)->save();
                }
            }
        } catch (Exception $ex){
            $this->_log('triggerCartUpdated', '', $ex->getMessage(), null, Zend_Log::ERR);
        }
    }

    public function triggerSubscribeUpdate($observer)
    {
        /**
         * @var $subscriber Mage_Newsletter_Model_Subscriber
         */
        $subscriber = $observer->getEvent()->getSubscriber();
        $this->_subscriber = $subscriber;

        if(!$this->_configHelp->isStoreInstalled($this->_subscriber->getStoreId())){
            return $this;
        }

        $loggedIn = Mage::getSingleton('customer/session')->isLoggedIn();

        if ($this->_subscriber->getId() && !$loggedIn) {
            if ($this->_subscriber->getCustomerId() && Mage::registry('remarkety_customer_save_observer_executed_' . $this->_subscriber->getCustomerId())) {
                return $this;
            }
            // Avoid loops - If this unsubsribe was triggered by remarkety, no need to update us
            if (Mage::registry('remarkety_subscriber_deleted')) {
                return $this;
            }

            $new = $subscriber->isObjectNew();


            if($this->_subscriber->getCustomerId()){
                $customer = Mage::getModel('customer/customer')->load($this->_subscriber->getCustomerId());
                $data = $this->serializeCustomer($customer);
                Mage::register(
                    'remarkety_customer_save_observer_executed_' . $customer->getId(),
                    true
                );

            } else {
                $data = $this->_prepareCustomerSubscribtionUpdateData(true);
            }
            $this->makeRequest(
                $new ? self::EVENT_CUSTOMERS_CREATED : self::EVENT_CUSTOMERS_UPDATED,
                $data,
                $this->_subscriber->getStoreId()
            );

            $email = $this->_subscriber->getSubscriberEmail();
            if (!empty($email)) {

                //save email to cart if needed
                $cart = Mage::getSingleton('checkout/session')->getQuote();
                if ($cart && !is_null($cart->getId()) && is_null($cart->getCustomerEmail())) {
                    $cart->setCustomerEmail($email)->save();
                }

                Mage::getSingleton('customer/session')->setSubscriberEmail($email);
            }
        }

        return $this;
    }

    public function triggerSubscribeDelete($observer)
    {
        $this->_subscriber = $observer->getEvent()->getSubscriber();
        if(!$this->_configHelp->isStoreInstalled($this->_subscriber->getStoreId())){
            return $this;
        }

        if (!Mage::registry('remarkety_subscriber_deleted_' . $this->_subscriber->getEmail()) && $this->_subscriber->getId()) {

            $this->makeRequest(
                self::EVENT_CUSTOMERS_UPDATED,
                $this->_prepareCustomerSubscribtionDeleteData(),
                $this->_subscriber->getStoreId()
            );
        }

        return $this;
    }

    public function triggerCustomerDelete($observer)
    {
        $this->_customer = $observer->getEvent()->getCustomer();
        if (!$this->_customer->getId()) {
            return $this;
        }

        if(!$this->_configHelp->isStoreInstalled($this->_customer->getStore()->getId())){
            return $this;
        }

        $this->makeRequest(
            self::EVENT_CUSTOMERS_DELETE,
            array(
                'id' => (int)$this->_customer->getId(),
                'email' => $this->_customer->getEmail()
            ), $this->_customer->getStore()->getId()
        );

        return $this;
    }

    public function triggerProductDeleteBefore($observer){
        /**
         * @var $product Mage_Catalog_Model_Product
         */
        $product = $observer->getProduct();
        $this->_productStoreIds = $product->getStoreIds();
    }
    public function triggerProductDelete($observer){


        /**
         * @var $product Mage_Catalog_Model_Product
         */
        $product = $observer->getProduct();
        //should send this event to all relevant stores
        foreach($this->_productStoreIds as $storeId) {
            if($this->_configHelp->isWebhooksEnabled($storeId)) {
                $this->makeRequest(self::EVENT_PRODUCTS_DELETED, array(
                    'id' => $product->getId()
                ), $storeId);
            }
        }
        return $this;
    }
    public function triggerCategoryChangeProducts($observer){
        if(!$this->shouldSendProductUpdates()){
            return $this;
        }

        $ids = $observer->getProductIds();
        if(!empty($ids)){
            foreach ($ids as $productId){
                $product = $this->_rmCore->loadProduct($productId);
                $this->productUpdate($product, $product->getStoreIds(), self::EVENT_PRODUCTS_UPDATED);
            }
        }
    }

    public function triggerProductSaveBefore($observer){
        /**
         * We need to store the original store ids to make sure to update removed store
         * Also in commit_after we cant get the store ids for some reason
         */
        if(!$this->shouldSendProductUpdates()){
            return $this;
        }
        $product = $observer->getProduct();

        $dbStores = array();
        if ($product->getOrigData() !== null) {
            $productDb = Mage::getModel("catalog/product")->load($product->getId());
            $dbStores = $productDb->getStoreIds();
        }
        $ids = array_merge($dbStores, $product->getStoreIds());
        $this->_productStoreIds = array_unique($ids);
    }
    public function triggerProductSaveCommit($observer){
        if(!$this->shouldSendProductUpdates()){
            return $this;
        }
        $product = $observer->getProduct();
        $storeIds = $this->_productStoreIds;
        if ($product->getOrigData() === null) {
            $eventType = self::EVENT_PRODUCTS_CREATED;
        } else {
            $eventType = self::EVENT_PRODUCTS_UPDATED;
        }
        $this->productUpdate($product, $storeIds, $eventType);

        return $this;
    }

    private function productUpdate($product, $storeIds, $eventType){
        $childProducts = array();
        $grouped = array(
            Mage_Catalog_Model_Product_Type::TYPE_CONFIGURABLE,
            Mage_Catalog_Model_Product_Type::TYPE_GROUPED
        );
        if(in_array($product->getTypeId(), $grouped)){
            $childProductsIds = Mage::helper('mgconnector/links')->getSimpleIds($product->getId());
            if(!empty($childProductsIds)) {
                foreach ($childProductsIds as $childProductsId) {
                    $childProducts[] = $this->_rmCore->loadProduct($childProductsId);
                }
            }
        }

        foreach($storeIds as $storeId) {
            if ($this->_configHelp->isWebhooksEnabled($storeId)) {
                $data = $this->serializeProduct($product, $storeId);
                $this->makeRequest($eventType, $data, $storeId);
                if(!empty($childProducts)){
                    foreach ($childProducts as $childProduct) {
                        $data = $this->serializeProduct($childProduct, $storeId);
                        $this->makeRequest($eventType, $data, $storeId);
                    }
                }
            }
        }
    }

    protected function _customerRegistration()
    {
        $this->makeRequest(
            self::EVENT_CUSTOMERS_CREATED,
            $this->_prepareCustomerUpdateData(),
            $this->_customer->getStore()->getId()
        );

        return $this;
    }

    protected function _customerUpdate()
    {
        if ($this->_hasDataChanged()) {
            $this->makeRequest(
                self::EVENT_CUSTOMERS_UPDATED,
                $this->_prepareCustomerUpdateData(),
                $this->_customer->getStore()->getId()
            );
            return true;
        }

        return false;
    }

    protected function _hasDataChanged()
    {
        if (!$this->_hasDataChanged && $this->_customer) {
            $validate = array(
                'firstname',
                'lastname',
                'title',
                'birthday',
                'gender',
                'email',
                'group_id',
                'default_billing',
                'is_subscribed',
            );
            $originalData = $this->_customer->getOrigData();
            $currentData = $this->_customer->getData();
            foreach ($validate as $field) {
                if (isset($originalData[$field])) {
                    if (!isset($currentData[$field]) || $currentData[$field] != $originalData[$field]) {
                        $this->_hasDataChanged = true;
                        break;
                    }
                }
            }
            // This part has been replaced by the loop above to avoid comparing objects in array_diff
            // $customerDiffKeys = array_keys( array_diff($this->_customer->getData(), $this->_customer->getOrigData()) );
            //
            // if(array_intersect($customerDiffKeys, $validate)) {
            //     $this->_hasDataChanged = true;
            // }
            $customerData = $this->_customer->getData();
            if (!$this->_hasDataChanged && isset($customerData['is_subscribed'])) {
                $subscriber = Mage::getModel('newsletter/subscriber')->loadByEmail($this->_customer->getEmail());
                $isSubscribed = $subscriber->getId() ? $subscriber->getData('subscriber_status') == Mage_Newsletter_Model_Subscriber::STATUS_SUBSCRIBED : false;

                if ($customerData['is_subscribed'] !== $isSubscribed) {
                    $this->_hasDataChanged = true;
                }
            }
        }
        if (!$this->_hasDataChanged && $this->_address && empty($this->_origAddressData)) {
            //new address
            $this->_hasDataChanged = true;
        }
        if (!$this->_hasDataChanged && $this->_address && $this->_origAddressData) {
            $validate = array(
                'street',
                'city',
                'region',
                'postcode',
                'country_id',
                'telephone',
                'firstname',
                'lastname'
            );
            $addressDiffKeys = array_keys(
                array_diff(
                    $this->_address->getData(),
                    $this->_origAddressData
                )
            );

            if (array_intersect($addressDiffKeys, $validate)) {
                $this->_hasDataChanged = true;
            }
        }

        return $this->_hasDataChanged;
    }

    protected function _getRequestConfig($eventType)
    {
        return array(
            'adapter' => 'Zend_Http_Client_Adapter_Curl',
            'curloptions' => array(
                // CURLOPT_FOLLOWLOCATION => true,
                CURLOPT_HEADER => true,
                CURLOPT_CONNECTTIMEOUT => self::REMARKETY_TIMEOUT,
                CURLOPT_SSL_VERIFYPEER => false
                // CURLOPT_SSL_CIPHER_LIST => "RC4-SHA"
            ),
        );
    }

    protected function _getHeaders($eventType, $payload, $storeId = null)
    {
        $domain = Mage::getBaseUrl(Mage_Core_Model_Store::URL_TYPE_WEB);
        $url = parse_url($domain);
        $domain = isset($url['host']) ? $url['host'] : $domain;

        $headers = array(
            'X-Domain: ' . $domain,
            'X-Token: ' . $this->_token,
            'X-Event-Type: ' . $eventType,
            'X-Platform: ' . self::REMARKETY_PLATFORM,
            'X-Version: ' . self::REMARKETY_VERSION,
        );
        if (isset($payload['storeId'])) {
            $headers[] = 'X-Magento-Store-Id: ' . $payload['storeId'];
        } elseif (isset($payload['store_id'])) {
            $headers[] = 'X-Magento-Store-Id: ' . $payload['store_id'];
        } elseif (!empty($storeId)){
            $headers[] = 'X-Magento-Store-Id: ' . $storeId;
        } else {
            $store = Mage::app()->getStore();
            $headers[] = 'X-Magento-Store-Id: ' . $store->getId();
        }

        return $headers;
    }

    protected function shouldSendEvent($eventType, $payload, $storeId = null){
        $data = array(
            'eventType' => $eventType,
            'payload' => $payload,
            'storeId' => $storeId
        );
        $hash = md5(serialize($data));
        if(array_key_exists($hash, $this->_sentEventsHash)){
            return false;
        }
        $this->_sentEventsHash[$hash] = true;
        return true;
    }
    public function makeRequest(
        $eventType,
        $payload,
        $storeId,
        $attempt = 1,
        $queueId = null
    ) {
        try {
            if(!$this->shouldSendEvent($eventType, $payload, $storeId)){
                //safety for not sending the same event on same event
                $this->_log(__FUNCTION__, '', 'Event already sent', $payload);
                return true;
            }

            if($this->_forceAsync && $attempt == 1 && empty($queueId)){
                $this->_queueRequest($eventType, $payload, 0, $queueId, $storeId);
                return true;
            }

            $url = self::REMARKETY_EVENTS_ENDPOINT;
            $storeAppId = $this->_configHelp->getRemarketyPublicId($storeId);
            if($storeAppId){
                $url .= '?storeId=' . $storeAppId;
            }

            $client = new Zend_Http_Client(
                $url,
                $this->_getRequestConfig($eventType)
            );
            $payload = array_merge(
                $payload,
                $this->_getPayloadBase($eventType)
            );
            $headers = $this->_getHeaders($eventType, $payload, $storeId);
            unset($payload['storeId']);
            $json = json_encode($payload);

            $response = $client
                ->setHeaders($headers)
                ->setRawData($json, 'application/json')
                ->request(self::REMARKETY_METHOD);

            Mage::log(
                "Sent event to endpoint: " . $json . "; Response (" . $response->getStatus() . "): " . $response->getBody(),
                \Zend_Log::DEBUG, REMARKETY_LOG
            );

            switch ($response->getStatus()) {
                case '200':
                    return true;
                case '400':
                    throw new Exception('Request has been malformed.');
                case '401':
                    throw new Exception('Request failed, probably wrong API key or inactive account.');
                default:
                    $error = $response->getStatus() . ' ' . $response->getBody();
                    $this->_queueRequest(
                        $eventType,
                        $payload,
                        $attempt,
                        $queueId,
                        $storeId,
                        $error
                    );
            }
        } catch (Exception $e) {
            $this->_queueRequest($eventType, $payload, $attempt, $queueId, $storeId, $e->getMessage());
        }

        return false;
    }

    protected function _queueRequest($eventType, $payload, $attempt, $queueId, $storeId = null, $errorMessage = null)
    {
        $queueModel = Mage::getModel('mgconnector/queue');

        if($attempt === 0 || !empty($this->_intervals[$attempt-1])) {
            $now = time();
            $nextAttempt = $now;
            if($attempt !== 0){
                $nextAttempt = $now + (int)$this->_intervals[$attempt - 1] * 60;
            }
            if ($queueId) {
                $queueModel->load($queueId);
                $queueModel->setAttempts($attempt);
                $queueModel->setLastAttempt(date("Y-m-d H:i:s", $now));
                $queueModel->setNextAttempt(date("Y-m-d H:i:s", $nextAttempt));
                $queueModel->setStoreId($storeId);
                $queueModel->setLastErrorMessage($errorMessage);
            } else {
                $queueModel->setData(
                    array(
                        'event_type' => $eventType,
                        'payload' => serialize($payload),
                        'attempts' => $attempt,
                        'last_attempt' => date("Y-m-d H:i:s", $now),
                        'next_attempt' => date("Y-m-d H:i:s", $nextAttempt),
                        'store_id' => $storeId,
                        'last_error_message' => $errorMessage
                    )
                );
            }
            return $queueModel->save();
        } elseif ($queueId) {
            $queueModel->load($queueId);
            $queueModel->setStatus(0);
            return $queueModel->save();
        }
        return false;
    }

    protected function _getPayloadBase($eventType)
    {
        date_default_timezone_set('UTC');
        $arr = array(
            'timestamp' => (string)time(),
            'event_id' => $eventType,
        );
        return $arr;
    }

    protected function _prepareCustomerUpdateData()
    {
        $data = $this->serializeCustomer($this->_customer);
        $data['storeId'] = $this->_customer->getStore()->getId();
        return $data;
    }

    protected function _getCustomerProductTags($customer = null)
    {
        if(is_null($customer)){
            $customer = $this->_customer;
        }
        $tags = Mage::getModel('tag/tag')->getResourceCollection();
        if (!empty($tags)) {
            $tags = $tags
                ->joinRel()
                ->addCustomerFilter($customer->getId());
        }

        return $tags;
    }

    protected function _prepareCustomerSubscribtionUpdateData(
        $newsletter = false
    ) {
        $quote = Mage::getSingleton('checkout/session')->getQuote();
        $store = Mage::app()->getStore();

        $arr = array(
            'email' => $this->_subscriber->getSubscriberEmail(),
            'accepts_marketing' => $this->_subscriber->getData('subscriber_status') == Mage_Newsletter_Model_Subscriber::STATUS_SUBSCRIBED,
            'storeId' => $store->getStoreId(),
        );

        if ($newsletter && (!is_object($quote) || $quote->getCheckoutMethod() !== Mage_Sales_Model_Quote::CHECKOUT_METHOD_GUEST)) {
            $arr['is_newsletter_subscriber'] = true;
        }

        return $arr;
    }

    protected function _prepareCustomerSubscribtionDeleteData()
    {
        $store = Mage::app()->getStore();

        $arr = array(
            'email' => $this->_subscriber->getSubscriberEmail(),
            'accepts_marketing' => false,
            'storeId' => $store->getStoreId()
        );

        return $arr;
    }

    private function deleteQueueItem($itemId){
        Mage::getModel('mgconnector/queue')
            ->load($itemId)
            ->delete();
    }

    public function resend($queueItems, $resetAttempts = false)
    {
        $sent = 0;
        foreach ($queueItems as $_queue) {
            $result = false;
            if($_queue->getEventType() == "catalogruleupdated"){
                //create queue for price rule update
                $ruleData = unserialize($_queue->getPayload());

                //delete the event from queue to make sure it's not running twice
                $this->deleteQueueItem($_queue->getId());
                $sent++;

                $ruleId = isset($ruleData['ruleId']) ? $ruleData['ruleId'] : false;
                if($ruleId){
                    $result = $this->sendProductPrices($ruleId);
                }
            } else {
                //send event to remarkety
                $result = $this->makeRequest(
                    $_queue->getEventType(),
                    unserialize($_queue->getPayload()),
                    $_queue->getStoreId(),
                    $resetAttempts ? 1 : ($_queue->getAttempts() + 1),
                    $_queue->getId()
                );
                if ($result) {
                    //delete the event from queue on success
                    $this->deleteQueueItem($_queue->getId());
                    $sent++;
                }
            }

        }

        return $sent;
    }

    public function run()
    {
        $collection = Mage::getModel('mgconnector/queue')->getCollection();
        $nextAttempt = date("Y-m-d H:i:s");
        $collection
            ->getSelect()
            ->where('next_attempt <= ?', $nextAttempt)
            ->where('status = 1')
            ->order('main_table.next_attempt asc');
        $this->resend($collection);

        return $this;
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

    protected function _productsUpdate($storeId, $data, $toQueue = false)
    {
        if($toQueue){
            $this->_queueRequest(self::EVENT_PRODUCTS_UPDATED, array('products' => $data), 1, null, $storeId);
        } else {
            $this->makeRequest(self::EVENT_PRODUCTS_UPDATED, array('products' => $data), $storeId);
        }
        return $this;
    }


    public function sendProductPrices($ruleId = null)
    {
        if(!$this->shouldSendProductUpdates()){
            return false;
        }
        // Fix for scenario when method is called directly as cron.
        if (is_object($ruleId)) {
            $ruleId = null;
        }

        $yesterday_start = date('Y-m-d 00:00:00',strtotime("-1 days"));
        $yesterday_end   = date('Y-m-d 23:59:59',strtotime("-1 days"));
        $today_start     = date('Y-m-d 00:00:00');
        $today_end       = date('Y-m-d 23:59:59');

        Mage::log('sendProductPrices started', null, 'remarkety-ext.log');

        $collection = Mage::getModel('catalogrule/rule')->getCollection();
        $collection->getSelect()
            ->joinLeft(
                array('catalogrule_product' => Mage::getSingleton('core/resource')->getTableName('catalogrule/rule_product')),
                'main_table.rule_id = catalogrule_product.rule_id',
                array('product_id')
            )
            ->group(array('main_table.rule_id', 'catalogrule_product.product_id'));

        if(is_null($ruleId)){
            $collection->getSelect()
                ->where('(main_table.from_date >= ?', $today_start)->where('main_table.from_date <= ?)', $today_end)
                ->orWhere('(main_table.to_date >= ? ',$yesterday_start)->where('main_table.to_date <= ?)', $yesterday_end)
                ->orWhere('(main_table.updated_at >= ? ',$yesterday_start)->where('main_table.updated_at <= ?)', $yesterday_end);
        } else {
            $collection->getSelect()
                ->where('main_table.rule_id = ?', $ruleId);
        }
        $useQueue = !is_null($ruleId);

//        $i = 0;
        $ruleProducts = array();
        foreach($collection->getData() as $c) {
            if (!isset($ruleProducts[$c['rule_id']]))
                $ruleProducts[$c['rule_id']] = array();
            $ruleProducts[$c['rule_id']][] = $c['product_id'];
        }

        $storeUrls = array();
        foreach($ruleProducts as $ruleId => $products) {
            /**
             * @var Mage_CatalogRule_Model_Rule
             */
            $catalog_rule = Mage::getModel('catalogrule/rule')->load($ruleId);
            $websiteIds = $catalog_rule->getWebsiteIds();
            foreach ($websiteIds as $websiteId) {
                $website = Mage::getModel('core/website')->load($websiteId);
                foreach ($website->getGroups() as $group) {
                    $stores = $group->getStores();
                    foreach ($stores as $store) {
                        if(!isset($storeUrls[$store->getStoreId()]))
                            $storeUrls[$store->getStoreId()] = Mage::app()->getStore($store->getStoreId())->getBaseUrl(Mage_Core_Model_Store::URL_TYPE_WEB, true);
                        $configInstalled = $store->getConfig(Remarkety_Mgconnector_Model_Install::XPATH_INSTALLED);
                        $isRemarketyInstalled = !empty($configInstalled);
                        if ($isRemarketyInstalled) {
                            $rows = array();
                            $i = 0;
                            foreach($products as $productId){
                                if($i >= 10){
                                    $this->_productsUpdate($store->getStoreId(), $rows, $useQueue);
                                    $i = 0;
                                    $rows = array();
                                }
                                $product = Mage::getModel('catalog/product')->load($productId);
                                $pWebsites = $product->getWebsiteIds();
                                if(in_array($websiteId, $pWebsites)) {
                                    $rows[] = $this->serializeProduct($product, $store->getStoreId());
                                    $i++;
                                }
                            }
                            if($i > 0){
                                $this->_productsUpdate($store->getStoreId(), $rows, $useQueue);
                            }
                        }
                    }
                }
            }
        }
        return true;
    }

    public function catalogInventorySave(Varien_Event_Observer $observer)
    {
        if ($this->shouldSendProductUpdates()) {
            $event = $observer->getEvent();
            $_item = $event->getItem();

            if ((int)$_item->getData('qty') != (int)$_item->getOrigData('qty')) {
                $product = $this->_rmCore->loadProduct($_item->getProductId());
                $this->productUpdate($product, $product->getStoreIds(), self::EVENT_PRODUCTS_UPDATED);
            }
        }
    }

    public function subtractQuoteInventory(Varien_Event_Observer $observer)
    {
        if ($this->shouldSendProductUpdates()) {
            $quote = $observer->getEvent()->getQuote();
            foreach ($quote->getAllItems() as $item) {
                $product = $this->_rmCore->loadProduct($item->getProductId());
                $this->productUpdate($product, $product->getStoreIds(), self::EVENT_PRODUCTS_UPDATED);
            }
        }
    }

    public function revertQuoteInventory(Varien_Event_Observer $observer)
    {
        if ($this->shouldSendProductUpdates()) {
            $quote = $observer->getEvent()->getQuote();
            foreach ($quote->getAllItems() as $item) {
                $product = $this->_rmCore->loadProduct($item->getProductId());
                $this->productUpdate($product, $product->getStoreIds(), self::EVENT_PRODUCTS_UPDATED);
            }
        }
    }

    public function cancelOrderItem(Varien_Event_Observer $observer)
    {
        //update products inventory when item is removed from order
        if ($this->shouldSendProductUpdates()) {
            $item = $observer->getEvent()->getItem();
            $product = $this->_rmCore->loadProduct($item->getProductId());
            $this->productUpdate($product, $product->getStoreIds(), self::EVENT_PRODUCTS_UPDATED);
        }
    }

    public function refundOrderInventory(Varien_Event_Observer $observer)
    {
        //update products inventory when order is refunded
        if ($this->shouldSendProductUpdates()) {
            $creditmemo = $observer->getEvent()->getCreditmemo();
            foreach ($creditmemo->getAllItems() as $item) {
                $product = $this->_rmCore->loadProduct($item->getProductId());
                $this->productUpdate($product, $product->getStoreIds(), self::EVENT_PRODUCTS_UPDATED);
            }
        }
    }

    private function shouldSendProductUpdates($storeId = null){
        if($this->_configHelp->isWebhooksEnabled($storeId)){
            return $this->_configHelp->isProductWebhooksEnabled();
        }
        return false;
    }
}
