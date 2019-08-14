<?php

/**
 * Install model
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Model_Install
{
    /**
     * Upgrade mode code
     */
    const MODE_UPGRADE = 'upgrade';

    /**
     * Install create mode code
     */
    const MODE_INSTALL_CREATE = 'install_create';

    /**
     * Install login mode code
     */
    const MODE_INSTALL_LOGIN = 'install_login';

    /**
     * Completed mode code
     */
    const MODE_COMPLETE = 'complete';

    /**
     * Welcome mode code
     */
    const MODE_WELCOME = 'welcome';

    /**
     * Web service remarkety username
     */
    const WEB_SERVICE_USERNAME = 'remarkety';

    /**
     * Web service role name
     */
    const WEB_SERVICE_ROLE = 'remarkety';

    /**
     * Store code scope
     */
    const STORE_SCOPE = 'stores';

    /**
     * Key in config for installed flag
     */
    const XPATH_INSTALLED = 'remarkety/mgconnector/installed';

    /**
     * Install data
     *
     * @var array
     */
    protected $_data = null;

    /**
     * Set data
     *
     * @param   array $data
     * @return  Remarkety_Mgconnector_Model_Install
     */
    public function setData(array $data)
    {
        $this->_data['mode'] = $data['mode'];
        $this->_data['email']= array_key_exists('email', $data) ? $data['email'] : null;
        $this->_data['first_name']= array_key_exists('first_name', $data) ? $data['first_name'] : null;
        $this->_data['last_name']= array_key_exists('last_name', $data) ? $data['last_name'] : null;
        $this->_data['phone']= array_key_exists('phone', $data) ? $data['phone'] : null;
        $this->_data['password']= array_key_exists('password', $data) ? $data['password'] : null;
        $this->_data['terms'] = array_key_exists('terms', $data) ? ($data['terms']== '1' ? 'true' : 'false') : null;
        $this->_data['store_id'] = array_key_exists('store_id', $data) ? $data['store_id'] : null;
        $this->_data['key'] = array_key_exists('key', $data) ? $data['key'] : null;

        return $this;
    }

    /**
     * Get data
     *
     * @return array
     */
    public function getData()
    {
        return $this->_data;
    }

    /**
     * Create web service role and user
     *
     * @throws Exception
     * @throws Mage_Core_Exception
     * @return Remarkety_Mgconnector_Model_Install
     */
    protected function _webServiceConfiguration()
    {
        $this->_data['key'] = $this->_generateApiKey();
        Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/api_key', $this->_data['key']);

        $wsFirstName = array_key_exists('first_name', $this->_data) && !empty($this->_data['first_name']) ? $this->_data['first_name'] : "Remarkety";
        $wsLastName = array_key_exists('last_name', $this->_data) && !empty($this->_data['last_name']) ? $this->_data['last_name'] : "API";

        if(!$this->_getWebServiceUser()->getId()) {
            $email = $this->_data['email'];

            $role = Mage::getModel('api/roles')
                ->setName(self::WEB_SERVICE_ROLE)
                ->setPid(false)
                ->setRoleType('G')
                ->save();

            Mage::getModel("api/rules")
                ->setRoleId($role->getId())
                ->setResources(array('all'))
                ->saveRel();

            $user = Mage::getModel('api/user')
                ->setData(
                    array(
                    'username' => self::WEB_SERVICE_ROLE,
                    'firstname' => $wsFirstName,
                    'lastname' => $wsLastName,
                    'email' => $email,
                    'api_key' => $this->_data['key'],
                    'api_key_confirmation' => $this->_data['key'],
                    'is_active' => 1,
                    'user_roles' => '',
                    'assigned_user_role' => '',
                    'role_name' => '',
                    'roles' => array($role->getId())
                    )
                );

            $retries = 0;
            $maxRetries = 5;
            while ($user->userExists() && $retries++ < $maxRetries) {
                $email = "_$email";
                $user->setData("email", $email);
            }

            if ($retries == $maxRetries) {
                throw new Exception("Could not create WebService user - all emails are taken");
            }

            $user->save();

            $user
                ->setRoleIds(array($role->getId()))
                ->setRoleUserId($user->getUserId())
                ->saveRelations();
        } else {
            $this
                ->_getWebServiceUser()
                //->setEmail($this->_data['email'])
                ->setFirstname($wsFirstName)
                ->setLastname($wsLastName)
                ->setNewApiKey($this->_data['key'])
                ->save();
        }

        return $this;
    }

    /**
     * Send request
     *
     * @param   $payload
     * @return  Remarkety_Mgconnector_Model_Install
     */
    protected function _sendRequest($payload)
    {
        Mage::getModel('mgconnector/request')->makeRequest($payload);

        return $this;
    }

    /**
     * Install extension creating new remarkety account
     *
     * @return Remarkety_Mgconnector_Model_Install
     * @throws Mage_Core_Exception
     */
    public function installByCreateExtension()
    {
        $connection = Mage::getSingleton('core/resource')->getConnection('core_write');

        try {
            $connection->beginTransaction();

            $this->_webServiceConfiguration();

            $connection->commit();
        } catch(Mage_Core_Exception $e) {
            $connection->rollback();
            throw new Mage_Core_Exception($e->getMessage());
        }

        // Make sure that store_id entry is an array
        if(!empty($this->_data['store_id']) && !is_array($this->_data['store_id'])) {
            $this->_data['store_id'] = (array)$this->_data['store_id'];
        }

        // Create request for each store view separately
        foreach($this->_data['store_id'] as $_storeId) {
            $store = Mage::getModel('core/store')->load($_storeId);
            $this->_sendRequest(
                array(
                'key' => $this->_data['key'],
                'email' => $this->_data['email'],
                'password' => $this->_data['password'],
                'acceptTerms' => $this->_data['terms'],
                'selectedView' => json_encode(
                    array(
                    'website_id' => $store->getWebsiteId(),
                    'store_id' => $store->getGroupId(),
                    'view_id' => $_storeId,
                    )
                ),
                'isNewUser' => true,
                'firstName' => $this->_data['first_name'],
                'lastName' => $this->_data['last_name'],
                'phone' => $this->_data['phone'],
                'storeFrontUrl' => $store->getBaseUrl(Mage_Core_Model_Store::URL_TYPE_LINK),
                'viewName' => $store->name,
                'ip'    => Mage::helper('core/http')->getRemoteAddr()
                )
            );

            $this->_markInstalled($_storeId);
        }

        // Reinitialize configuration
        Mage::app()->getCacheInstance()->cleanType('config');
        Mage::dispatchEvent('adminhtml_cache_refresh_type', array('type' => 'config'));

        return $this;
    }

    /**
     * Install extension using existing remarkety account
     *
     * @return Remarkety_Mgconnector_Model_Install
     * @throws Mage_Core_Exception
     */
    public function installByLoginExtension()
    {
        $connection = Mage::getSingleton('core/resource')->getConnection('core_write');

        try {
            $connection->beginTransaction();

            $this->_webServiceConfiguration();

            $connection->commit();
        } catch(Mage_Core_Exception $e) {
            $connection->rollback();
            throw new Mage_Core_Exception($e->getMessage());
        }

        // Make sure that store_id entry is an array
        if(!empty($this->_data['store_id']) && !is_array($this->_data['store_id'])) {
            $this->_data['store_id'] = (array)$this->_data['store_id'];
        }

        // Create request for each store view separately
        foreach($this->_data['store_id'] as $_storeId) {
            $store = Mage::getModel('core/store')->load($_storeId);

            $this->_sendRequest(
                array(
                'key' => $this->_data['key'],
                'email' => $this->_data['email'],
                'password' => $this->_data['password'],
                'acceptTerms' => $this->_data['terms'],
                'selectedView' => json_encode(
                    array(
                    'website_id' => $store->getWebsiteId(),
                    'store_id' => $store->getGroupId(),
                    'view_id' => $_storeId,
                    )
                ),
                'isNewUser' => false,
                'storeFrontUrl' => $store->getBaseUrl(Mage_Core_Model_Store::URL_TYPE_LINK),
                'viewName' => $store->name,
                'ip'    => Mage::helper('core/http')->getRemoteAddr()
                )
            );

            $this->_markInstalled($_storeId);
        }

        // Reinitialize configuration
        Mage::app()->getCacheInstance()->cleanType('config');
        Mage::dispatchEvent('adminhtml_cache_refresh_type', array('type' => 'config'));

        return $this;
    }

    /**
     * Upgrade extension
     *
     * @return Remarkety_Mgconnector_Model_Install
     * @throws Mage_Core_Exception
     */
    public function upgradeExtension()
    {
        $connection = Mage::getSingleton('core/resource')->getConnection('core_write');

        try {
            $connection->beginTransaction();

            $webServiceUser = $this->_getWebServiceUser();

            $this->_data['email'] = $webServiceUser->getEmail();
            $this->_data['key'] = $this->_generateApiKey();

            $this
                ->_getWebServiceUser()
                ->setNewApiKey($this->_data['key'])
                ->save();

            Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/api_key', $this->_data['key']);

            $connection->commit();
        } catch(Mage_Core_Exception $e) {
            $connection->rollback();
            throw new Mage_Core_Exception($e->getMessage());
        }

        $this->_sendRequest(array());

        return $this;
    }

    /**
     * Complete extension installation
     *
     * @return Remarkety_Mgconnector_Model_Install
     * @throws Mage_Core_Exception
     */
    public function completeExtensionInstallation()
    {
        $connection = Mage::getSingleton('core/resource')->getConnection('core_write');

        try {
            $connection->beginTransaction();

            $ver = Mage::getConfig()->getModuleConfig("Remarkety_Mgconnector")->version;
            Mage::getModel('core/config')->saveConfig(self::XPATH_INSTALLED, $ver);

            $intervals = Mage::getStoreConfig('mgconnector_options/mgconnector_options_group/intervals');
            if(!empty($intervals)) {
                Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/intervals', $intervals);
            } else {
                Mage::getModel('core/config')->saveConfig('remarkety/mgconnector/intervals', "1,3,10");
            }

            // remove old config entries if exist
            Mage::getModel('core/config')
                ->deleteConfig('mgconnector_options/mgconnector_options_group/api_key')
                ->deleteConfig('mgconnector_options/mgconnector_options_group/intervals');

            // remove old files
            $blockDir = Mage::getModuleDir('Block', 'Remarkety_Mgconnector') . DS . 'Block';
            $etcDir = Mage::getModuleDir('etc', 'Remarkety_Mgconnector');

            if(file_exists($file = $etcDir . DS . 'system.xml')) {
                unlink($file);
            }

            if(file_exists($file = $blockDir . DS . 'Adminhtml' . DS . 'Mgconnector.php')) {
                unlink($file);
            }

            if(file_exists($file = $blockDir . DS . 'Adminhtml' . DS . 'Mgconnector' . DS . 'Grid.php')) {
                unlink($file);
            }

            if(file_exists($file = $blockDir . DS . 'Adminhtml' . DS . 'Mgconnector' . DS . 'Grid' . DS . 'Column' . DS . 'Renderer' . DS . 'Status.php')) {
                unlink($file);
            }

            $connection->commit();
        } catch(Mage_Core_Exception $e) {
            $connection->rollback();
            throw new Mage_Core_Exception($e->getMessage());
        }

        return $this;
    }

    /**
     * Generate new api key
     *
     * @return string
     * @throws Mage_Core_Exception
     */
    protected function _generateApiKey()
    {
        $apiKey = Mage::getStoreConfig('remarkety/mgconnector/api_key');
        if (!empty($apiKey))
            return $apiKey;
        if(!empty($this->_data['email'])) {
            return md5($this->_data['email'] . time());
        }

        throw new Mage_Core_Exception('Can not generate api key');
    }

    /**
     * Return remarkety webservice user
     *
     * @return Mage_Api_Model_User
     */
    protected function _getWebServiceUser()
    {
        $webServiceUser = Mage::getModel('api/user')
            ->loadByUsername(self::WEB_SERVICE_USERNAME);

        return $webServiceUser;
    }

    /**
     * Return remarkety webservice user by email
     *
     * @param   string $email
     * @return  Mage_Api_Model_User
     */
    protected function _getWebServiceUserByEmail($email) 
    {
        $webServiceUser = Mage::getModel('api/user')
            ->loadByEmail($email);

        return $webServiceUser;
    }

    /**
     * Mark extension as installed for provided store id
     *
     * @param   int $storeId
     * @throws  Mage_Core_Exception
     * @return  Remarkety_Mgconnector_Model_Install
     */
    protected function _markInstalled($storeId) 
    {
        $connection = Mage::getSingleton('core/resource')->getConnection('core_write');

        try {
            $connection->beginTransaction();

            $ver = Mage::getConfig()->getModuleConfig("Remarkety_Mgconnector")->version;
            Mage::getModel('core/config')->saveConfig(
                self::XPATH_INSTALLED,
                $ver,
                self::STORE_SCOPE,
                $storeId
            );

            $response = Mage::getSingleton('core/session')->getRemarketyLastResponseMessage();
            $response = !empty($response) ? unserialize($response) : array();
            if(!empty($response['storePublicId'])){
                /**
                 * @var $m Remarkety_Mgconnector_Model_Webtracking
                 */
                $m = Mage::getModel('mgconnector/webtracking');
                $m->setRemarketyPublicId($storeId, $response['storePublicId']);
            }

            $connection->commit();
        } catch(Mage_Core_Exception $e) {
            $connection->rollback();
            throw new Mage_Core_Exception($e->getMessage());
        }

        return $this;
    }

    /**
     * Return core_config_data entries for stores where extension is installed
     *
     * @return object
     */
    public function getConfiguredStores()
    {
        $collection = Mage::getModel('core/config_data')->getCollection();
        $collection
            ->getSelect()
            ->where('scope = ?', self::STORE_SCOPE)
            ->where('path = ?', self::XPATH_INSTALLED);

        return $collection;
    }

    public static function isMultipleStores() 
    {
        return 'true' === 'true';
    }
}
