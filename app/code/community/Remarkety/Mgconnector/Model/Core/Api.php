<?php

/**
 * @copyright    Copyright (C) 2014 InteraMind Ltd (Remarkety). All rights reserved.
 * @license    http://www.gnu.org/licenses/gpl-2.0.html GNU/GPL
 * @package    This file is part of Magento Connector Plugin for Remarkety
 **/
class Remarkety_Mgconnector_Model_Core_Api
{
    public function getCustomers(
        $mage_store_group_id,
        $updated_at_min = null,
        $updated_at_max = null,
        $limit = null,
        $page = null,
        $since_id = null,
        $returnLog = 0
) 
    { 
     
     
     
     
     
     
     
     
    
    

        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();

        return $myModel->getCustomers(
            $mage_store_group_id,
            $updated_at_min,
            $updated_at_max,
            $limit,
            $page,
            $since_id
        );
    }

    public function getOrders(
        $mage_store_group_id,
        $updated_at_min = null,
        $updated_at_max = null,
        $limit = null,
        $page = null,
        $since_id = null,
        $created_at_min = null,
        $created_at_max = null,
        $order_status = null,
        // Not implemented
        $order_id = null,
        $returnLog = 0
) 
    { 
     
     
     
     
     
     
     
     
    
    

        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();

        return $myModel->getOrders(
            $mage_store_group_id,
            $updated_at_min,
            $updated_at_max,
            $limit,
            $page,
            $since_id,
            $created_at_min,
            $created_at_max,
            $order_status,        // Not implemented
            $order_id
        );
    }

    public function getQuotes(
        $mage_store_group_id,
        $updated_at_min = null,
        $updated_at_max = null,
        $limit = null,
        $page = null,
        $since_id = null,
        $returnLog = 0
) 
    { 
     
     
     
     
     
     
     
     
    
    

        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();

        return $myModel->getQuotes(
            $mage_store_group_id,
            $updated_at_min,
            $updated_at_max,
            $limit,
            $page,
            $since_id
        );
    }

    public function getProducts(
        $mage_store_view_id,
        $updated_at_min = null,
        $updated_at_max = null,
        $limit = null,
        $page = null,
        $handle = null,
        // Not implemented
        $vendor = null,
        // Not implemented
        $product_type = null,
        // Not implemented
        $collection_id = null,
        // Not implemented
        $since_id = null,
        $created_at_min = null,
        $created_at_max = null,
        $published_status = null,
        // Not implemented
        $product_id = null,
        $returnLog = 0
) 
    { 
     
     
     
     
     
     
     
     
    
    

        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();
        return $myModel->getProducts(
            $mage_store_view_id,
            $updated_at_min,
            $updated_at_max,
            $limit,
            $page,
            $handle,
            $vendor,
            $product_type,
            $collection_id,
            $since_id,
            $created_at_min,
            $created_at_max,
            $published_status,
            $product_id,
            $returnLog
        );
    }

    public function getSubscribers(
        $mage_store_view_id = null,
        $limit = null,
        $page = null,
        $sinceId = null,
        $returnLog = 0
) 
    { 
     
     
     
     
     
     
     
     
    
    
        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();

        return $myModel->getSubscribers(
            $mage_store_view_id,
            $limit,
            $page,
            $sinceId
        );
    }
    
    public function getSubscriberCount($mage_store_view_id = null, $returnLog = 0)
    {
    
        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();
    
        return $myModel->getSubscriberCount($mage_store_view_id);
    }
    
    public function getStoreSettings($mage_store_view_id = null, $returnLog = 0)
    {
        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();
        return $myModel->getStoreSettings($mage_store_view_id);
    }

    public function getStoreOrderStatuses($mage_store_view_id, $returnLog = 0)
    {
        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();
        return $myModel->getStoreOrderStatuses($mage_store_view_id);
    }

    public function createCoupon($rule_id, $coupon_code, $expiration, $returnLog = 0)
    {
        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();
        $ret = $myModel->createCoupon($rule_id, $coupon_code, $expiration);
        return $ret;
    }

    public function unsubscribe($email, $returnLog = 0)
    {
        $myModel = Mage::getModel('mgconnector/core');
        if ($returnLog)
            $myModel->sendLogInResponse();
        $ret = $myModel->unsubscribe($email);
        return $ret;
    }

    public function getCustomersCount($mage_store_group_id, $returnLog = 0)
    {
        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();
        return $myModel->getCustomersCount($mage_store_group_id);
    }

    public function getOrdersCount($mage_store_group_id, $returnLog = 0)
    {
        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();
        return $myModel->getOrdersCount($mage_store_group_id);
    }

    public function getProductsCount($mage_view_id, $returnLog = 0)
    {
        $myModel = Mage::getModel("mgconnector/core");
        if ($returnLog)
            $myModel->sendLogInResponse();
        $ret = $myModel->getProductsCount($mage_view_id);
        return $ret;
    }
}
