<?php
class Remarkety_Mgconnector_Block_Tracking_Product extends Remarkety_Mgconnector_Block_Tracking_Base
{
    private $_CategoryNames = array();
    private $_CategoryIds = array();
    /**
     * Get the active product object
     *
     * @return Mage_Catalog_Model_Product
     */
    public function getActiveProduct()
    {
        if($this->getProduct()){
            return $this->getProduct();
        }

        return Mage::registry('product');
    }

    /**
     * Get arrays of Category ids and names
     * @return array
     */
    public function getCategories()
    {
        if(empty($this->_CategoryNames) || empty($this->_CategoryIds)) {
            $cats = $this->getActiveProduct()->getCategoryIds();
            $productCategoryNames = Array();
            foreach ($cats as $category_id) {
                $_cat = Mage::getModel('catalog/category')->load($category_id);
                $productCategoryNames[] = $_cat->getName();
            }

            $this->_CategoryNames = $productCategoryNames;
            $this->_CategoryIds = $cats;
        }

        return Array(
            "ids" => $this->_CategoryIds,
            "names" => $this->_CategoryNames
        );
    }

    /**
     * Return array of Category names
     * @return array
     */
    public function getCategoryNames()
    {
        $info = $this->getCategories();
        return $info['names'];
    }

    /**
     * Return array of Category ids
     * @return array
     */
    public function getCategoryIds()
    {
        $info = $this->getCategories();
        return $info['ids'];
    }
}
