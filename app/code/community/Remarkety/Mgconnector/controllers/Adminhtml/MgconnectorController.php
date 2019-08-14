<?php

/**
 * Adminhtml Mgconnector controller
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Adminhtml_MgconnectorController extends Mage_Adminhtml_Controller_Action
{

    /**
     * Is allowed action
     *
     * @return bool
     */

    protected function _isAllowed() 
    {
        return true;
    }

    /**
     * Init action
     *
     * @return Remarkety_Mgconnector_Adminhtml_MgconnectorController
     */
    protected function _initAction()
    {
        $this
            ->loadLayout()
            ->_title($this->__('Remarkety'))
            ->_setActiveMenu('mgconnector');

        return $this;
    }

    /**
     * Index action
     */
    public function indexAction()
    {
        $this
            ->_initAction()
            ->_title($this->__('Queue'));

        $this->_addContent($this->getLayout()->createBlock('mgconnector/adminhtml_queue'));
        $this->renderLayout();
    }

    /**
     * Reinstall action
     */
    public function reinstallAction()
    {
        Mage::getModel('core/config')
            ->deleteConfig('remarkety/mgconnector/apikey')
            ->deleteConfig('remarkety/mgconnector/last_response')
            ->deleteConfig(Remarkety_Mgconnector_Model_Install::XPATH_INSTALLED)
            ->deleteConfig('remarkety/mgconnector/intervals');

        $this->_redirect('*/*/installation');
    }

    /**
     * Configuration action
     */
    public function configurationAction()
    {
        $mode = $this->_initMode();

        if($mode === Remarkety_Mgconnector_Model_Install::MODE_CONFIGURATION) {
            if($this->getRequest()->isPost()) {
                $params = $this->getRequest()->getParams();

                Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/intervals', $params['data']['intervals']);
                $this->_getSession()->addSuccess($this->__('Configuration has been saved.'));

                $this->_redirect('*/*/*');
            } else {
                $this
                    ->_initAction()
                    ->_title($this->__(ucfirst($mode)));

                $this->_addContent($this->getLayout()->createBlock(sprintf('mgconnector/adminhtml_configuration_%s', $mode)));
                $this->renderLayout();
            }
        } else {
            $this->_redirect('*/*/installation', array('_current' => true));
        }
    }

    /**
     * Installation action
     */
    public function installationAction()
    {
        $mode = $this->_initMode();

        $this
            ->_initAction()
            ->_title($this->__(ucfirst($mode)));

        $this->_addContent($this->getLayout()->createBlock(sprintf('mgconnector/adminhtml_configuration_%s', $mode)));
        $this->renderLayout();
    }

    /**
     * Complete action
     */
    public function completeAction()
    {
        $redirectBack = true;

        if($this->getRequest()->isPost()) {
            $params = $this->getRequest()->getParams();

            try {
                $install = Mage::getModel('mgconnector/install')
                    ->setData($params['data']);   

                switch($params['data']['mode']) {
                    case Remarkety_Mgconnector_Model_Install::MODE_INSTALL:
                        $install->installExtension();
                        break;
                    case Remarkety_Mgconnector_Model_Install::MODE_UPGRADE:
                        $install->upgradeExtension();
                        break;
                    case Remarkety_Mgconnector_Model_Install::MODE_COMPLETE:
                        $install->completeExtensionInstallation();
                        break;
                    default:
                        throw new Mage_Core_Exception('Selected mode can not be handled.');
                }

                $redirectBack = false;
            } catch(Mage_Core_Exception $e) {
                $this->_getSession()->addError($e->getMessage());
            }
        }

        $this->_redirect(sprintf('*/*/%s', $redirectBack ? 'installation' : 'configuration'));
    }

    /**
     * Grid action
     */
    public function gridAction()
    {
        $this->loadLayout();
        $this->getResponse()->setBody(
            $this->getLayout()->createBlock('mgconnector/adminhtml_mgconnector_grid')->toHtml()
        );
    }

    /**
     * Return mode
     * @return string
     * @throws Mage_Core_Exception
     */
    protected function _initMode()
    {
        $mode = Remarkety_Mgconnector_Model_Install::MODE_INSTALL;

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

        $response = Mage::getStoreConfig('remarkety/mgconnector/last_response');
        if(!empty($response)) {
            $mode = Remarkety_Mgconnector_Model_Install::MODE_COMPLETE;
        }

        $installed = Mage::getStoreConfig(Remarkety_Mgconnector_Model_Install::XPATH_INSTALLED);
        if(!empty($installed)) {
            $mode = Remarkety_Mgconnector_Model_Install::MODE_CONFIGURATION;
        }

        $forceMode = $this->getRequest()->getParam('mode', false);
        if(!empty($forceMode)) {
            $mode = $forceMode;
        }

        if(!in_array(
            $mode, array(
            Remarkety_Mgconnector_Model_Install::MODE_INSTALL,
            Remarkety_Mgconnector_Model_Install::MODE_UPGRADE,
            Remarkety_Mgconnector_Model_Install::MODE_COMPLETE,
            Remarkety_Mgconnector_Model_Install::MODE_CONFIGURATION
            )
        )) {
            throw new Mage_Core_Exception('Installation mode can not be handled.');
        }

        return $mode;
    }
}
