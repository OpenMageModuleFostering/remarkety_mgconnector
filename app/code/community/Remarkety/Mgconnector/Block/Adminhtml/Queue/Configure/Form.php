<?php

/**
 * Adminhtml configure form block
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Queue_Configure_Form extends Mage_Adminhtml_Block_Widget_Form
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
            'action' => $this->getUrl('*/queue/save'),
            'method' => 'post',
            )
        );
        $form->setFieldContainerIdPrefix('data');
        $form->setUseContainer(true);
        $this->setForm($form);

        $fieldset = $form->addFieldset(
            'general',
            array(
                'legend' => $this->__('Remarkety Configuration')
            )
        );

        $fieldset->addField(
            'mode', 'hidden', array(
            'name' => 'data[mode]',
            'value' => 'configuration',
            )
        );
        
        if(!$this->checkAPIKey()) {
            $fieldset->addField(
                'api_warning', 'note', array(
                'text' => '<span style="color: red;">' . $this->__('Attention! Remarkety\'s API key is not the same as the Magento API key. Please set it below.') . '</span>'.
                ' <a target="_blank" href="https://support.remarkety.com/hc/en-us/articles/209184646-Synchronizing-the-API-key">More Info</a>.',
                )
            );
        }

        $fieldset->addField(
            'api_key', 'text', array(
            'label' => $this->__('API Key:'),
            'name' => 'data[api_key]',
            'required' => true,
            'after_element_html' => '<small style="float:left;width:100%;">' . $this->__(
                'This API key will be used for Remarkety\'s sync and queue.'
            ) . '</small>',
            'value' => Mage::getStoreConfig('remarkety/mgconnector/api_key'),
            'style' => 'float:left',
            )
        );

        $fieldset->addField(
            'intervals', 'text', array(
            'label' => $this->__('Intervals:'),
            'name' => 'data[intervals]',
            'required' => false,
            'after_element_html' => '<small style="float:left;width:100%;">' . $this->__(
                'This defines the queue retry interval.<br/>
            		Type the amount of minutes between retries, separated by commas. For example "1,3,10" -
                    the second attempt will be after 1 minute, third after 3 minutes,<br/>
                    and fourth after 10 minutes. If the last attempt is unsuccessful,
                    the status will be changed to "failed" and will not be retried anymore.'
            ) . '</small>',
            'value' => Mage::getStoreConfig('remarkety/mgconnector/intervals'),
            'style' => 'float:left',
            )
        );

        $fieldset->addField(
            'bypasscache', 'checkbox', array(
            'label' => $this->__('Bypass page cache for website tracking:'),
            'name' => 'data[bypasscache]',
            'after_element_html' => '<small style="float:left;width:100%;">' . $this->__(
                'Enable this checkbox if you use caching on your store pages.'
            ) . '</small>',
            'value' => 1,
            'checked' => \Remarkety_Mgconnector_Model_Webtracking::getBypassCache() ? 'checked' : '',
            'style' => 'float:left',
            )
        );

        $fieldset->addField(
            'markgroupparent', 'checkbox', array(
            'label' => $this->__('Mark grouped product as parent of associated products'),
            'name' => 'data[markgroupparent]',
            'after_element_html' => '<small style="float:left;width:100%;">' . $this->__(
                'Enable this checkbox only if each simple product has only 1 parent grouped product'
            ) . '</small>',
            'value' => 1,
            'checked' => Mage::getStoreConfig('remarkety/mgconnector/mark_group_parent') ? 'checked' : '',
            'style' => 'float:left',
            )
        );

        $fieldset->addField(
            'simpleproductstandalone', 'checkbox', array(
            'label' => $this->__('Get images and urls independently for simple products'),
            'name' => 'data[simpleproductstandalone]',
            'after_element_html' => '<small style="float:left;width:100%;">' . $this->__(
                'Enable this checkbox if simple products that are related to configurable products have their own images and urls'
            ) . '</small>',
            'value' => 1,
            'checked' => Mage::getStoreConfig('remarkety/mgconnector/configurable_standalone') ? 'checked' : '',
            'style' => 'float:left',
            )
        );

        $button = $fieldset->addField(
            'button', 'note', array(
            'label' => false,
            'name' => 'button',
            'after_element_html' => '<button type="button" class="save" onclick="editForm.submit();"><span><span>'
                . $this->__('Save') . '</span></span></button>',
            )
        );
        $button->getRenderer()->setTemplate('mgconnector/element.phtml');

        return parent::_prepareForm();
    }

    private function checkAPIKey()
    {
        try {
            $uModel = Mage::getModel('api/user');
            $apiKey = Mage::getStoreConfig('remarkety/mgconnector/api_key');
            return $uModel->authenticate(\Remarkety_Mgconnector_Model_Install::WEB_SERVICE_USERNAME, $apiKey);
        } catch (Exception $ex){
            return false;
        }
    }
}
