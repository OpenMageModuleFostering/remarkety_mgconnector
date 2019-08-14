<?php

class Remarkety_Mgconnector_Helper_Urls extends Mage_Core_Helper_Abstract
{
    private $_attributeId = false;
    private $_urlKey = false;

    public function getValue($productId, $store_id)
    {
        if(isset($this->_urlKey[$store_id]) && isset($this->_urlKey[$store_id][$productId])) {
            return $this->_urlKey[$store_id][$productId];
        }

        $connection = Mage::getSingleton('core/resource')->getConnection('core_read');
        $tableName = Mage::getSingleton('core/resource')->getTableName('catalog_product_entity_varchar');
        $attrbuteId = $this->_getAttributeId();
        $query = 'SELECT `value`, `store_id` FROM ' . $tableName . ' WHERE (store_id = "'.$store_id.'" OR store_id = 0) AND attribute_id = "' . $attrbuteId . '" AND entity_id = "'. $productId .'"';

        $value = $connection->fetchAll($query);

        if(count($value) > 0) {
            if(count($value) == 1) {
                $this->_urlKey[$store_id][$productId] = $value[0]['value'];
            } else {
                foreach($value AS $sValue) {
                    if($sValue['store_id'] != 0) {
                        $this->_urlKey[$store_id][$productId] = $sValue['value'];
                    }
                }
            }

            return $this->_urlKey[$store_id][$productId];
        }

        return false;
    }

    private function _getAttributeId() 
    {
        if(!$this->_attributeId) {
            $this->_attributeId = Mage::getResourceModel('eav/entity_attribute')->getIdByCode('catalog_product', 'url_key');
        }

        return $this->_attributeId;
    }


}
