<?php

/**
 * Adminhtml Queue controller
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Adminhtml_QueueController extends Mage_Adminhtml_Controller_Action
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
     * @return Remarkety_Mgconnector_Adminhtml_QueueController
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
            ->_title($this->__('Queue'))
            ->_addContent($this->getLayout()->createBlock('mgconnector/adminhtml_queue_configure'))
            ->_addContent($this->getLayout()->createBlock('mgconnector/adminhtml_queue'))
            ->renderLayout();
    }

    public function saveAction()
    {
        if($this->getRequest()->isPost()) {
            $params = $this->getRequest()->getParams();
    
            Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/intervals', $params['data']['intervals']);

            if(isset($params['data']['api_key'])){
                $key = $params['data']['api_key'];
                Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/api_key', $key);
                $apiUser = Mage::getModel('api/user')->loadByUsername(\Remarkety_Mgconnector_Model_Install::WEB_SERVICE_USERNAME);
                $apiUser
                    ->setNewApiKey($key)
                    ->save();
            }

            if(isset($params['data']['bypasscache'])){
                Mage::getModel('core/config')->saveConfig(\Remarkety_Mgconnector_Model_Webtracking::RM_BYPASS_CACHE, true);
            } else {
                Mage::getModel('core/config')->saveConfig(\Remarkety_Mgconnector_Model_Webtracking::RM_BYPASS_CACHE, false);
            }

            if(isset($params['data']['markgroupparent'])){
                Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/mark_group_parent', true);
            } else {
                Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/mark_group_parent', false);
            }

            if(isset($params['data']['simpleproductstandalone'])){
                Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/configurable_standalone', true);
            } else {
                Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/configurable_standalone', false);
            }

            Mage::app()->getCacheInstance()->cleanType('config');
            Mage::dispatchEvent('adminhtml_cache_refresh_type', array('type' => 'config'));

            $this->_getSession()->addSuccess($this->__('Configuration has been saved.'));
        }
    
        $this->_redirect('*/queue/index');
    }
    
    /**
     * Grid action
     */
    public function gridAction()
    {
        $this->loadLayout();
        $this->getResponse()->setBody(
            $this->getLayout()->createBlock('mgconnector/adminhtml_queue_grid')->toHtml()
        );
    }
    
    public function massResendAction() 
    {
        
        $queueIds = $this->getRequest()->getParam('queue');
        if (!is_array($queueIds)) {
            Mage::getSingleton('adminhtml/session')->addError($this->__('Please select queue item(s)'));
        }
        else {
            try {
                $collection = Mage::getModel('mgconnector/queue')->getCollection();
                $collection
                    ->addFieldToFilter('queue_id', $queueIds)
                    ->getSelect();
                $observer = Mage::getModel('mgconnector/observer');
                $itemsSent = $observer->resend($collection);
                Mage::getSingleton('adminhtml/session')->addSuccess(
                    Mage::helper('adminhtml')->__('Total of %d events(s) were resent', $itemsSent)
                );
            } catch (Exception $e) {
                Mage::getSingleton('adminhtml/session')->addError($e->getMessage());
            }
        }
        
        $this->_redirect('*/queue/index');
    }
    
    public function massDeleteAction() 
    {
        
        $queueIds = $this->getRequest()->getParam('queue');
        if (!is_array($queueIds)) {
            Mage::getSingleton('adminhtml/session')->addError($this->__('Please select queue item(s)'));
        }
        else {
            try {
                $collection = Mage::getModel('mgconnector/queue')->getCollection();
                $collection
                    ->addFieldToFilter('queue_id', $queueIds)
                    ->getSelect();
                foreach ($collection as $item) {
                    $item->delete();
                }

                Mage::getSingleton('adminhtml/session')->addSuccess(
                    Mage::helper('adminhtml')->__('Total of %d events(s) were deleted', count($queueIds))
                );
            } catch (Exception $e) {
                Mage::getSingleton('adminhtml/session')->addError($e->getMessage());
            }
        }
        
        $this->_redirect('*/queue/index');
    }
}
