<?php

/**
 * Remarkety Mgconnector recovery controller.
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_WebtrackingController extends Mage_Core_Controller_Front_Action
{
    public function identifyAction()
    {
        $this->getResponse()->clearHeaders()->setHeader('Content-type', 'application/x-javascript', true);
        $email = $this->getEmail();
        if($email){
            $this->getResponse()->setBody('_rmData.push(["setCustomer", "'.$email.'"]);');
        } else {
            $this->getResponse()->setBody('');
        }

    }

    private function getEmail()
    {
        if(Mage::getSingleton('customer/session')->isLoggedIn()){
            $customer = Mage::getSingleton('customer/session')->getCustomer();
            return $customer->getEmail();
        }

        $email = Mage::getSingleton('customer/session')->getSubscriberEmail();
        return empty($email) ? false : $email;
    }
}
