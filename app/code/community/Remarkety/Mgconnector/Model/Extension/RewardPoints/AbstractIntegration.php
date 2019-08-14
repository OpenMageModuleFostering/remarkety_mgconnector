<?php

/**
 * Reward points integration abstract class.
 *
 * @category Remarkety
 * @package  Remarkety_Mgconnector
 * @author   Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
abstract class Remarkety_Mgconnector_Model_Extension_RewardPoints_AbstractIntegration
    implements Remarkety_Mgconnector_Model_Extension_RewardPoints_IntegrationInterface
{
    /**
     * Helper object.
     *
     * @var Remarkety_Mgconnector_Helper_Extension
     */
    protected $helper;

    /**
     * Helper getter.
     *
     * @return Remarkety_Mgconnector_Helper_Extension
     */
    public function getHelper()
    {
        if (is_null($this->helper)) {
            $this->helper = Mage::helper('mgconnector/extension');
        }

        return $this->helper;
    }

    /**
     * Method returns bool value if magento will find and consider
     * extension as enabled.
     *
     * @return bool
     */
    public function isModuleEnabled()
    {
        return $this->getHelper()->isModuleEnabled($this->getModuleName());
    }

    /**
     * Return store config.
     *
     * @param string $fieldPath Field path.
     * @param mixed  $store     Store data.
     *
     * @return bool
     */
    protected function getStoreConfig($fieldPath, $store = null)
    {
        return Mage::getStoreConfigFlag(
            $fieldPath,
            $store
        );
    }
}
