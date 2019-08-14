<?php

/**
 * Abstract extension controller.
 *
 * @category Remarkety
 * @package  Remarkety_Mgconnector
 * @author   Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
abstract class Remarkety_Mgconnector_Adminhtml_Extension_AbstractController
    extends Mage_Adminhtml_Controller_Action
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
     * Init action.
     *
     * @return Remarkety_Mgconnector_Adminhtml_Extension_AbstractController
     */
    protected function initAction()
    {
        $this
            ->loadLayout()
            ->_title($this->__('Remarkety'))
            ->_setActiveMenu('mgconnector');

        return $this;
    }
}
