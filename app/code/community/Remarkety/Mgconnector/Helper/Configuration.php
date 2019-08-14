<?php

class Remarkety_Mgconnector_Helper_Configuration extends Mage_Core_Helper_Abstract
{
    private $_cache = array();
    private $_installedStores = false;

    const XPATH_INTERNAL_PRODUCT_URL = 'remarkety/mgconnector/product_url_internal';
    const XPATH_SHARED_CUSTOMERS = 'remarkety/mgconnector/allow_shared_customers';
    const XPATH_FORCE_ORDER_UPDATES = 'remarkety/mgconnector/force_order_updates';

    public function getValue($key, $default = null)
    {
        // Check if the key is in our cache
        if (key_exists($key, $this->_cache))
            return $this->_cache[$key];

        // Check if the key was sent in the headers
        $value = Mage::app()->getRequest()->getParam($key);
        if (!is_null($value)) {
            $this->_cache[$key] = $value;
            return $value;
        }

        // Check if the key is in a configuration
        $value = Mage::getStoreConfig("remarkety/mgconnector/$key");
        if (!is_null($value)) {
            $this->_cache[$key] = $value;
            return $value;
        }

        return $default;

    }

    /**
     * Get a list of stores that has Remarkety installed
     * @return array
     */
    public function getInstalledStores(){
        if($this->_installedStores === false) {
            $this->_installedStores = array();
            $stores = Mage::app()->getStores(false);
            /**
             * @var $store Mage_Core_Model_Store
             */
            foreach ($stores as $storeId => $store) {
                $isInstalled = $store->getConfig(Remarkety_Mgconnector_Model_Install::XPATH_INSTALLED);
                if(!empty($isInstalled)){
                    $this->_installedStores[] = $storeId;
                }
            }
        }
        return $this->_installedStores;
    }

    /**
     * Check if a specific store id has Remarkety installed
     * @param $storeId
     * @return bool
     */
    public function isStoreInstalled($storeId){
        if(!$this->_installedStores) {
            /**
             * @var $store Mage_Core_Model_Store
             */
            $store = Mage::app()->getStore($storeId);
            $isInstalled = $store->getConfig(Remarkety_Mgconnector_Model_Install::XPATH_INSTALLED);
            return !empty($isInstalled);
        }
        return in_array($storeId, $this->getInstalledStores());
    }

    /**
     * Check if a specific store id has Remarkety installed
     * @param $storeId
     * @return bool
     */
    public function isWebhooksEnabled($storeId = null){
            /**
             * @var $store Mage_Core_Model_Store
             */
            $isInstalled = Mage::getStoreConfig(Remarkety_Mgconnector_Model_Install::XPATH_WEBHOOKS_ENABLED);

            if(!empty($isInstalled)){
                if(empty($storeId)){
                    return true;
                }

                $store = Mage::app()->getStore($storeId);
                $storeId = $this->getRemarketyPublicId($store);
                return !empty($storeId);
            }
            return false;
    }

    public function isProductWebhooksEnabled(){
        $isDisabled = Mage::getStoreConfig(Remarkety_Mgconnector_Model_Install::XPATH_PRODUCT_WEBHOOKS_DISABLED);
        return empty($isDisabled);
    }

    public function allowCustomerSharing(){
        $customerShare = Mage::getSingleton('customer/config_share');
        if($customerShare->isGlobalScope()) {
            return Mage::getStoreConfig(self::XPATH_SHARED_CUSTOMERS);
        }
        return false;
    }

    public function forceOrderUpdates(){
        return Mage::getStoreConfig(self::XPATH_FORCE_ORDER_UPDATES) == 1;
    }

    /**
     * @param null $store
     * @return bool
     */
    public function getRemarketyPublicId($store = null)
    {
        $store = is_null($store) ? Mage::app()->getStore() : $store;
        $store_id = is_numeric($store) ? $store : $store->getStoreId();
        $id = Mage::getStoreConfig(Remarkety_Mgconnector_Model_Webtracking::RM_STORE_ID, $store_id);
        return (empty($id) || is_null($id)) ? false : $id;
    }

    public static function getProductUrlFromMagento(){
        $val = Mage::getStoreConfig(self::XPATH_INTERNAL_PRODUCT_URL);
        return !empty($val);
    }
}
