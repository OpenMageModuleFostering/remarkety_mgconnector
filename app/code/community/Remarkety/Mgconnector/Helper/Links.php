<?php

class Remarkety_Mgconnector_Helper_Links extends Mage_Core_Helper_Abstract
{
    private $parentId = null;
    private $simpleIds = null;


    private function loadProductLinks()
    {

        if ($this->parentId && $this->simpleIds) {
            return;
        }

        $connection = Mage::getSingleton('core/resource')->getConnection('core_read');
        $tableName = Mage::getSingleton('core/resource')->getTableName('catalog_product_super_link');

        $query = 'select product_id,parent_id from ' . $tableName;
        $this->simpleIds = array();
        $this->parentId = array();

        foreach ($connection->fetchAll($query) as $row) {
            $productId = $row['product_id'];
            $parentId = $row['parent_id'];

            if (!isset($this->simpleIds[$parentId])) {
                $this->simpleIds[$parentId] = array();
            }
            $this->simpleIds[$parentId][] = $productId;
            $this->parentId[$productId] = $parentId;
        }
    }

    public function getParentId($productId = null)
    {
        $this->loadProductLinks();

        if (!$productId) {
            return false;
        }

        if (isset($this->parentId[$productId])) {
            return $this->parentId[$productId];
        }

        return null;
    }

    public function getSimpleIds($productId = null)
    {

        $this->loadProductLinks();

        if (!isset($productId)) {
            return $this->simpleIds;
        }

        if (isset($this->simpleIds[$productId])) {
            return $this->simpleIds[$productId];
        }
        return null;
    }
}