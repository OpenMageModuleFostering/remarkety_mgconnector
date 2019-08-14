<?php

class Remarkety_Mgconnector_Block_Adminhtml_Install_Welcome_Store extends Mage_Adminhtml_Block_Template
{
    /**
     * Prepare block
     */
    public function __construct()
    {
        $this->setTemplate('mgconnector/install/welcome/store.phtml');
        parent::__construct();
    }

    public function getStoresStatus()
    {
        /**
         * @var $wtModel Remarkety_Mgconnector_Model_Webtracking
         */
        $wtModel = Mage::getModel('mgconnector/webtracking');

        $stores = array();

        foreach (Mage::app()->getWebsites() as $_website) {
            $stores[$_website->getCode()] = array(
                'name' => $_website->getName(),
                'id' => $_website->getWebsiteId(),
            );

            foreach ($_website->getGroups() as $_group) {
                $stores[$_website->getCode()]['store_groups'][$_group->getGroupId()] = array(
                    'name' => $_group->getName(),
                    'id' => $_group->getGroupId(),
                );

                foreach ($_group->getStores() as $_store) {
                    $isInstalled = $_store->getConfig(Remarkety_Mgconnector_Model_Install::XPATH_INSTALLED);
                    $webtracking = $wtModel->getRemarketyPublicId($_store->getStoreId());
                    $stores[$_website->getCode()]['store_groups'][$_group->getGroupId()]['store_views'][$_store->getCode()] = array(
                        'name' => $_store->getName(),
                        'id' => $_store->getStoreId(),
                        'isInstalled' => $isInstalled,
                        'webTracking' => $webtracking
                    );
                }
            }
        }

        return $stores;
    }

    public function checkAPIKey()
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
