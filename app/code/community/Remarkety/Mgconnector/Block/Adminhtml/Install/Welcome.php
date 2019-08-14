<?php

/**
 * Adminhtml welcome complete block
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Install_Welcome extends Mage_Adminhtml_Block_Widget_Form_Container
{
    /**
     * Prepare block
     */
    public function __construct()
    {
        parent::__construct();

        $this->_objectId = 'welcome_id';
        $this->_blockGroup = 'mgconnector';
        $this->_controller = 'adminhtml_install';
        $this->_mode = 'welcome';

        $this->_headerText = $this->__(
            'Remarkety (version: %s)',
            Mage::helper('mgconnector')->getInstalledVersion()
        );

        $this->_removeButton('back');
        $this->_removeButton('reset');
        $this->_removeButton('save');

        $this->_addButton(
            'reinstall', array(
            'label'     => Mage::helper('adminhtml')->__('Reinstall'),
            'onclick'   => "return confirm('Are you sure?') ? window.location = '" . $this->getUrl('*/install/reinstall')."' : false;",
            'class'     => 'delete',
            ), 0
        );
    }
}
