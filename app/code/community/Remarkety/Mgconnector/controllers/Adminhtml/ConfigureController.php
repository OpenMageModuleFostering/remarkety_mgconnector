<?php

/**
 * Adminhtml Configure controller
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Adminhtml_ConfigureController extends Mage_Adminhtml_Controller_Action
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
     * @return Remarkety_Mgconnector_Adminhtml_ConfigureController
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
     * Configuration action
     */
    public function indexAction()
    {
        $mode = Mage::helper('mgconnector')->getMode();
        if($mode === Remarkety_Mgconnector_Model_Install::MODE_WELCOME) {
            $this
                ->_initAction()
                ->_title($this->__('Configuration'))
                ->_addContent($this->getLayout()->createBlock('mgconnector/adminhtml_configure'))
                ->renderLayout();
        } else {
            $this->_redirect('*/install/install');
        }
    }

    public function saveAction()
    {
        if($this->getRequest()->isPost()) {
            $params = $this->getRequest()->getParams();

            Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/intervals', $params['data']['intervals']);
            $this->_getSession()->addSuccess($this->__('Configuration has been saved.'));
        }

        $this->_redirect('*/queue/index');
    }
}
