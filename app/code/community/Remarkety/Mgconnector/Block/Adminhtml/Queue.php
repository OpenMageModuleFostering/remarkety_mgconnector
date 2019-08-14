<?php

/**
 * Adminhtml queue block
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Queue extends Mage_Adminhtml_Block_Widget_Grid_Container
{
    /**
     * Prepare block
     */
    public function __construct()
    {
        parent::__construct();

        $this->_blockGroup = 'mgconnector';
        $this->_controller = 'adminhtml_queue';
        $this->_headerText = Mage::helper('mgconnector')->__('Queue Contents');
        $this->_removeButton('add');
    }
}
