<?php

class Remarkety_Mgconnector_Helper_Configuration extends Mage_Core_Helper_Abstract
{
    private $_cache = array();

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
}
