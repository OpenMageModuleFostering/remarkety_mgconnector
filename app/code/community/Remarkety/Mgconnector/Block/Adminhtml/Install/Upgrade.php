<?php

/**
 * Adminhtml configuration upgrade block
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Install_Upgrade extends Mage_Adminhtml_Block_Widget_Form_Container
{
    /**
     * Prepare block
     */
    public function __construct()
    {
        parent::__construct();

        $this->_objectId = 'upgrade_id';
        $this->_blockGroup = 'mgconnector';
        $this->_controller = 'adminhtml_install';
        $this->_mode = 'upgrade';

        $this->_headerText = $this->__(
            'Upgrade Remarkety extension (version: %s)',
            Mage::helper('mgconnector')->getInstalledVersion()
        );

        $this->_removeButton('back');
        $this->_removeButton('reset');
        $this->_addButton(
            'save', array(
            'label'     => Mage::helper('adminhtml')->__('Complete Installation'),
            'onclick'   => 'editForm.submit();',
            'class'     => 'save',
            ), 1
        );
    }
}
