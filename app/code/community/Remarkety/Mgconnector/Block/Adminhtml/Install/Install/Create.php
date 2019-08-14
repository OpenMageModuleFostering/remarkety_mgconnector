<?php

/**
 * Adminhtml install install block
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Install_Install_Create extends Mage_Adminhtml_Block_Widget_Form_Container
{
    /**
     * Prepare block
     */
    public function __construct()
    {
        parent::__construct();

        $this->_objectId = 'install_id';
        $this->_blockGroup = 'mgconnector';
        $this->_controller = 'adminhtml_install_install';
        $this->_mode = 'create';

        $this->_headerText = $this->__(
            'Install Remarkety extension (version: %s)',
            Mage::helper('mgconnector')->getInstalledVersion()
        );

        $this->_removeButton('back');
        $this->_removeButton('reset');
        $this->_removeButton('save');
    }
}
