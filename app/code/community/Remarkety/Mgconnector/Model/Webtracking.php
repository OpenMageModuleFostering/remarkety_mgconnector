<?php

Class Remarkety_Mgconnector_Model_Webtracking extends Mage_Core_Model_Abstract
{
    const RM_STORE_ID = 'remarkety/mgconnector/public_storeId';
    const RM_BYPASS_CACHE = 'remarkety/mgconnector/bypass_cache';
    const STORE_SCOPE = 'stores';
    public function getRemarketyPublicId($store = null)
    {
        $store = is_null($store) ? Mage::app()->getStore() : $store;
        $store_id = is_numeric($store) ? $store : $store->getStoreId();
        $id = Mage::getStoreConfig(self::RM_STORE_ID, $store_id);
        return (empty($id) || is_null($id)) ? false : $id;
    }

    public function setRemarketyPublicId($store, $newId)
    {
        $store_id = is_numeric($store) ? $store : $store->getStoreId();
        Mage::getModel('core/config')->saveConfig(
            self::RM_STORE_ID,
            $newId,
            self::STORE_SCOPE,
            $store_id
        );
        Mage::app()->getCacheInstance()->cleanType('config');
        Mage::dispatchEvent('adminhtml_cache_refresh_type', array('type' => 'config'));
    }

    public static function getBypassCache()
    {
        return $apiKey = Mage::getStoreConfig(self::RM_BYPASS_CACHE);
    }

    /**
     * @param bool $bool_val
     */
    public function setBypassCache($bool_val)
    {
        Mage::getModel('core/config')->saveConfig(self::RM_BYPASS_CACHE, $bool_val);
    }
}
