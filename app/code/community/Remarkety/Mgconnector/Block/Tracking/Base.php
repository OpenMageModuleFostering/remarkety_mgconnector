<?php
class Remarkety_Mgconnector_Block_Tracking_Base extends Mage_Core_Block_Template
{
    public function isWebtrackingActivated()
    {
        return ($this->getRemarketyPublicId() !== false);
    }

    public function getRemarketyPublicId()
    {
        $store = Mage::app()->getStore();
        /**
         * @var $m Remarkety_Mgconnector_Model_Webtracking
         */
        $m = Mage::getModel('mgconnector/webtracking');
        return $m->getRemarketyPublicId($store);
    }

    public function shouldBypassCache()
    {
        return \Remarkety_Mgconnector_Model_Webtracking::getBypassCache();
    }

    public function getCustomer()
    {
        return Mage::getSingleton('customer/session')->getCustomer();
    }

    public function getEmail()
    {
        if(Mage::getSingleton('customer/session')->isLoggedIn()){
            return $this->getCustomer()->getEmail();
        }

        $email = Mage::getSingleton('customer/session')->getSubscriberEmail();
        return empty($email) ? false : $email;
    }

    public function getStoreBaseUrl(){
        $url = Mage::getBaseUrl(Mage_Core_Model_Store::URL_TYPE_LINK);
        $url = preg_replace('/^http(s?)\:\/\//i', '//', $url);
        return $url;
    }
}
