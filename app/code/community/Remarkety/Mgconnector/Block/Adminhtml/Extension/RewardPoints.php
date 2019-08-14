<?php

/**
 * Admin reward points extension block.
 *
 * @category Remarkety
 * @package  Remarkety_Mgconnector
 * @author   Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Extension_Rewardpoints
    extends Mage_Adminhtml_Block_Template
{
    /**
     * Internal constructor.
     *
     * @return void
     */
    protected function _construct()
    {
        parent::_construct();
    }

    /**
     * Return page header.
     *
     * @return string
     */
    public function getPageHeader()
    {
        return $this->__('Reward Points Extensions');
    }

    /**
     * Return read more url.
     *
     * @return string
     */
    public function getReadMoreUrl()
    {
        return 'https://support.remarkety.com/hc/en-us/articles/209584646';
    }

    /**
     * Return supported extension names.
     *
     * @return array
     */
    public function getSupportedExtensionNames()
    {
        $extensionsConfig = Mage::helper('mgconnector/extension')
            ->getRewardPointsExtensions();

        $extensionNames = array();
        foreach ($extensionsConfig as $extensionConfig) {
            $extensionNames[] = $this->__($extensionConfig['title']);
        }

        return $extensionNames;
    }

    /**
     * Return enabled extensions links html.
     *
     * @return string
     */
    public function getEnabledExtensionsListHtml()
    {
        $enabledExtensions = Mage::helper('mgconnector/extension')
            ->getEnabledRewardPointsExtensions();

        $links = array();
        foreach ($enabledExtensions as $enabledExtension) {
            $links[] = sprintf(
                '<a href="%s" target="_blank">%s</a>',
                $enabledExtension['url'],
                $this->__($enabledExtension['title'])
            );
        }

        return implode(', ', $links);
    }
}
