<?php

/**
 * Adminhtml welcome complete form block
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Install_Welcome_Form extends Mage_Adminhtml_Block_Widget_Form
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
            'action' => $this->getUrl('*/*/install'),
            'method' => 'post',
            )
        );
        $form->setFieldContainerIdPrefix('data');
        $form->setUseContainer(true);
        $this->setForm($form);

        $fieldset = $form->addFieldset(
            'general',
            array(
                'legend' => $this->__('Remarkety')
            )
        );

        $fieldset->addField(
            'mode', 'hidden', array(
            'name' => 'data[mode]',
            'value' => 'complete',
            )
        );

        $instruction = $fieldset->addField(
            'instruction', 'note', array(
            'text' => '',
            'label' => false,
            'after_element_html' => '<p style="font-weight:bold;font-size:25px;">' . $this->__('Welcome to Remarkety - What\'s next?') . '</p>
            <ol style="list-style-type:decimal;margin-left:20px;font-weight:bold;font-size:12px;">
                <li>Sign in to your account <a href="https://app.remarkety.com/?utm_source=plugin&utm_medium=link&utm_campaign=magento-plugin" target="_blank">here</a></li>
                <li>Create campaigns, send emails and monitor results.</li>
                <li>Increase sales and customer\'s Life Time Value</li>
                <li>Need help? We are here for you: <a href="mailto:support@remarkety.com">support@remarkety.com</a> <a href="tel:%28%2B1%20800%20570-7564">(+1 800 570-7564)</a></li>
            </ol>
            '
            )
        );
        $instruction->getRenderer()->setTemplate('mgconnector/element.phtml');

        return parent::_prepareForm();
    }
}
