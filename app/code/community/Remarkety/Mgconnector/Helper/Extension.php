<?php

/**
 * Extension integration helper.
 *
 * @category Remarkety
 * @package  Remarkety_Mgconnector
 * @author   Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Helper_Extension extends Mage_Core_Helper_Abstract
{
    const XML_EXTENSIONS_REWARD_POINTS
        = 'mgconnector_supported_extensions/rewardpoints';

    /**
     * Supported reward points extensions config.
     *
     * @var array
     */
    protected $rewardPointsExtensionsConfig;

    /**
     * Enabled reward points extensions config.
     *
     * @var array
     */
    protected $enabledRewardPointsExtensionsConfig;

    /**
     * Return supported reward points extensions config data.
     *
     * @return array
     */
    public function getRewardPointsExtensions()
    {
        if (is_null($this->rewardPointsExtensionsConfig)) {
            $extensionsConfig = Mage::getStoreConfig(
                self::XML_EXTENSIONS_REWARD_POINTS
            );
            if (!is_array($extensionsConfig)) {
                $extensionsConfig = array();
            }

            foreach ($extensionsConfig as $extensionCode => $extensionConfig) {
                $extensionsConfig[] = array_merge(
                    array('code' => $extensionConfig),
                    $extensionConfig
                );
                unset($extensionsConfig[$extensionCode]);
            }

            usort(
                $extensionsConfig,
                function ($a, $b) {
                    return (int)$a['sortOrder'] - (int)$b['sortOrder'];
                }
            );

            $this->rewardPointsExtensionsConfig = $extensionsConfig;
        }

        return $this->rewardPointsExtensionsConfig;
    }

    /**
     * Return enabled reward points extensions config data.
     *
     * @return array
     * @throws Mage_Core_Exception
     */
    public function getEnabledRewardPointsExtensions()
    {
        if (is_null($this->enabledRewardPointsExtensionsConfig)) {
            $extensionHelper = Mage::helper('mgconnector/extension');

            $extensionsConfig = $this->getRewardPointsExtensions();
            $enabledExtensionsConfig = array();
            foreach ($extensionsConfig as $extensionConfig) {
                $model = Mage::getModel($extensionConfig['model']);
                if (!$model instanceof Remarkety_Mgconnector_Model_Extension_RewardPoints_AbstractIntegration) {
                    throw new Mage_Core_Exception(
                        $extensionHelper->__('Unsupported integration model type.')
                    );
                }

                if ($model->isEnabled()) {
                    $enabledExtensionsConfig[] = $extensionConfig;
                }
            }

            $this->enabledRewardPointsExtensionsConfig = $enabledExtensionsConfig;
        }

        return $this->enabledRewardPointsExtensionsConfig;
    }

    /**
     * Return reward point integration model instance.
     * If there are no enabled reward points extensions enabled false
     * will be returned.
     * If there are more than one reward points extensions enabled,
     * first will be returned. Please keep in mind that they are sorted
     * by sortOrder value.
     *
     * @return bool|Mage_Core_Model_Abstract
     * @throws Mage_Core_Exception
     */
    public function getRewardPointsIntegrationInstance()
    {
        $enabledExtensions = $this->getEnabledRewardPointsExtensions();
        if (empty($enabledExtensions)) {
            return false;
        }

        foreach ($enabledExtensions as $enabledExtension) {
            return $model = Mage::getModel($enabledExtension['model']);
        }
    }
}
