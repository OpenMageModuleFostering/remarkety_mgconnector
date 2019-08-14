<?php

/**
 * Adminhtml configure block
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Queue_Configure extends Mage_Adminhtml_Block_Widget_Form_Container
{
    /**
     * Prepare block
     */
    public function __construct()
    {
        parent::__construct();

        $this->_objectId = 'configure_id';
        $this->_blockGroup = 'mgconnector';
        $this->_controller = 'adminhtml_queue';
        $this->_mode = 'configure';

        $this->_headerText = $this->__(
            'Configuration'
            //             Mage::helper('mgconnector')->getInstalledVersion()
        );

        $this->_removeButton('back');
        $this->_removeButton('reset');
        
//         $this->_addButton('save', array(
//             'label'     => Mage::helper('adminhtml')->__('Save'),
//             'onclick'   => 'editForm.submit();',
//             'class'     => 'save',
//         ), 0);
    }
}
