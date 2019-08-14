<?php

/**
 * Mgconnector helper
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Helper_Data extends Mage_Core_Helper_Abstract
{
    /**
     * Return installed extension version
     *
     * @return string
     */
    public function getInstalledVersion()
    {
        return Mage::getConfig()->getModuleConfig("Remarkety_Mgconnector")->version;
    }

    /**
     * Return mode
     *
     * @return  string
     * @throws  Mage_Core_Exception
     */
    public function getMode()
    {
        $mode = Remarkety_Mgconnector_Model_Install::MODE_INSTALL_CREATE;

        /**
         * If remarkety extension was installed in version lower than 1.0.0.15
         * and remarkety web service user already exists
         */
        $webServiceUser = Mage::getModel('api/user')
            ->setUsername(Remarkety_Mgconnector_Model_Install::WEB_SERVICE_USERNAME)
            ->userExists();

        if($webServiceUser === true) {
            $mode = Remarkety_Mgconnector_Model_Install::MODE_UPGRADE;
        }

        $response = Mage::getSingleton('core/session')->getRemarketyLastResponseStatus();
        //$response = Mage::getModel('core/config')->getConfig('remarkety/mgconnector/last_response_status');
        if($response == 1) {
            $mode = Remarkety_Mgconnector_Model_Install::MODE_COMPLETE;
        } elseif($response == 0) {
            $mode = Remarkety_Mgconnector_Model_Install::MODE_INSTALL_CREATE;
        }

        $configuredStores = Mage::getModel('mgconnector/install')->getConfiguredStores();
        if($configuredStores->getSize()) {
            $mode = Remarkety_Mgconnector_Model_Install::MODE_WELCOME;
        }

        $forceMode = Mage::app()->getRequest()->getParam('mode', false);
        if(!empty($forceMode)) {
            $mode = $forceMode;
        }

        if(!in_array(
            $mode, array(
            Remarkety_Mgconnector_Model_Install::MODE_INSTALL_CREATE,
            Remarkety_Mgconnector_Model_Install::MODE_INSTALL_LOGIN,
            Remarkety_Mgconnector_Model_Install::MODE_UPGRADE,
            Remarkety_Mgconnector_Model_Install::MODE_COMPLETE,
            Remarkety_Mgconnector_Model_Install::MODE_WELCOME,
            )
        )) {
            throw new Mage_Core_Exception('Installation mode can not be handled.');
        }

        return $mode;
    }
}
