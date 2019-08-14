<?php

/**
 * Adminhtml install install form block
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Install_Install_Login_Form extends Mage_Adminhtml_Block_Widget_Form
{
    /**
     * Prepare form
     *
     * @return Mage_Adminhtml_Block_Widget_Form
     */
    protected function _prepareForm()
    {
        $form = new Varien_Data_Form(
            array(
            'id' => 'edit_form',
            'action' => $this->getUrl('*/install/complete'),
            'method' => 'post',
            )
        );
        $form->setFieldContainerIdPrefix('data');
        $form->setUseContainer(true);
        $this->setForm($form);

        $fieldset = $form->addFieldset(
            'general',
            array(
                'legend' => $this->__('Install Remarkety extension')
            )
        );

        $fieldset->addField(
            'mode', 'hidden', array(
            'name' => 'data[mode]',
            'value' => 'install_login',
            )
        );
        $noAccountUrl = $this->getUrl('*/install/install', array('mode' => Remarkety_Mgconnector_Model_Install::MODE_INSTALL_CREATE));
        
        /*$headingHtml =
        '<p><b>' . $this->__('Thank you for installing the Remarkety Magento plugin.
                You are one click away from finishing setting up Remarkety on your store and sending effective, targeted emails!')
                        . '</b><br><br>'
                        		. $this->__('The plugin will automatically create a Magento WebService API user so that
                Remarkety can synchronize with your store.') . '</p><hr/>'
                        		. '<h2>'.$this->__('Login to Remarkety') . '</h2>'
                        				. '<p>'.
                        				sprintf($this->__(
                        						'Don\'t have a Remarkety account yet? <a href="%s">Click here</a>'
                        				), $noAccountUrl)
                        				. '</p>';
        
        /$instruction = $fieldset->addField('instruction', 'note', array(
            'text' => '',
            'label' => false,
            'after_element_html' => $headingHtml,
//                 '<p style="font-weight:bold;">' . $this->__('Thank you for installing the Remarkety Magento plugin.
//                 You are one click away from finishing setting up Remarkety on your store and sending effective, targeted emails!')
//                 . '<br><br>'
//                 . $this->__('The plugin will automatically create a Magento WebService API user so that
//                 Remarkety can synchronize with your store.') . '</p>',
        ));
        $instruction->getRenderer()->setTemplate('mgconnector/element.phtml');*/

        $noAccountUrl = $this->getUrl('*/install/install', array('mode' => Remarkety_Mgconnector_Model_Install::MODE_INSTALL_CREATE));
        
        $html = '<small style="float:left;width:100%;">' . sprintf(
            $this->__(
                'Don\'t have a Remarkety account yet? <a href="%s">Click here</a>'
            ), $noAccountUrl
        ) . '</small>';
        
        $fieldset->addField(
            'email', 'text', array(
            'label' => $this->__('Email address for the Remarkety account:'),
            'name' => 'data[email]',
            'required' => true,
            'class' => 'validate-email',
                /*
            'after_element_html' => $html,
            '<small style="float:left;width:100%;">' . $this->__(
                    'If you’ve already registered to Remarkety, please use the email you used to open your account.
                    If you haven’t, please click on the button "Create New Account And Install" below.'
                ) . '</small>',
               */
            'style' => 'float:left',
            )
        );

        $fieldset->addField(
            'password', 'password', array(
            'label' => $this->__('Password:'),
            'name' => 'data[password]',
            'required' => true,
            'class' => 'required-entry'
            )
        );

        $fieldset->addField(
            'store_id', 'select', array(
            'name' => 'data[store_id]',
            'label' => $this->__('Connect this view:'),
            'required' => true,
            'values' => Mage::getSingleton('adminhtml/system_store')->getStoreValuesForForm(false, false),
            //'value' => $this->getRequest()->getParam('store'),
            )
        );

//         $fieldset->addField('terms', 'checkbox', array(
//             'label' => false,
//             'name' => 'data[terms]',
//             'checked' => false,
//             'value' => '1',
//             'class' => 'required-entry',
//             'after_element_html' => $this->__('I agree to Remarkety’s <a href="%s">terms of use</a>.', '#'),
//         ));

        $fieldset->addField(
            'login', 'note', array(
            'label' => false,
            'name' => 'button',
            'after_element_html' => '<button type="button" class="save" onclick="editForm.submit();"><span>'
                . $this->__('Login And Connect') . '
                </span></button>',
            )
        );

//         $fieldset->addField('create', 'note', array(
//             'label' => false,
//             'name' => 'button',
//             'after_element_html' => '<button type="button" class="save"
//             onclick="window.location = \'' . $this->getUrl('*/install/install', array('mode' => Remarkety_Mgconnector_Model_Install::MODE_INSTALL_CREATE)) . '\'"
//             ><span><span>'
//                 . $this->__('Create New Account And Install') . '</span></span></button>',
//         ));

        return parent::_prepareForm();
    }
}
