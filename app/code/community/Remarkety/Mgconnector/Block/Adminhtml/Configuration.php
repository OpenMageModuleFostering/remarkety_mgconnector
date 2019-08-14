<?php

/**
 * Adminhtml configuration block
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Configuration extends Mage_Adminhtml_Block_Widget_Grid_Container
{
    /**
     * Prepare block
     */
    public function __construct()
    {
        $this->_blockGroup = 'mgconnector';
        $this->_controller = 'adminhtml_configuration';
        $this->_headerText = Mage::helper('mgconnector')->__('Remarkety Configuration');
        $this->_removeButton('add');

        parent::__construct();
    }
}
