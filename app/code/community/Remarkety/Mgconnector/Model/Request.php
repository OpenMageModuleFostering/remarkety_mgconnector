<?php

/**
 * Request model
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Model_Request
{
    const REMARKETY_URI = 'https://app.remarkety.com/public/install/notify';
    const REMARKETY_STOREID_URI = 'https://app.remarkety.com/public/install/get-store-id';
    const REMARKETY_METHOD = 'POST';
    const REMARKETY_TIMEOUT = 30;
    const REMARKETY_VERSION = 0.9;
    const REMARKETY_PLATFORM = 'MAGENTO';
    const REMARKETY_OEM = 'remarkety';

    protected function _getRequestConfig()
    {
        return array(
            'adapter' => 'Zend_Http_Client_Adapter_Curl',
            'curloptions' => array(
                CURLOPT_HEADER => true,
                CURLOPT_CONNECTTIMEOUT => self::REMARKETY_TIMEOUT,
            ),
            'timeout' => self::REMARKETY_TIMEOUT,
            'request_timeout' => self::REMARKETY_TIMEOUT
        );
    }

    protected function _getPayloadBase()
    {
        $domain = Mage::getBaseUrl(Mage_Core_Model_Store::URL_TYPE_WEB);
        $domain = substr($domain, 7, -1);

        $version = Mage::getVersion();
        $version .= ' ' . (Mage::helper('core')->isModuleEnabled('Enterprise_Enterprise') ? 'EE' : 'CE');

        $arr = array(
            'domain' => $domain,
            'platform' => Remarkety_Mgconnector_Model_Request::REMARKETY_PLATFORM,
            'version' => $version,
            'oem'       => Remarkety_Mgconnector_Model_Request::REMARKETY_OEM,
        );
        return $arr;
    }

    public function getStoreID($magento_store_id)
    {
        try {
            $store = Mage::getModel('core/store')->load($magento_store_id);
            $payload = $this->_getPayloadBase();

            $payload['selectedView'] = json_encode(
                array(
                'website_id' => $store->getWebsiteId(),
                'store_id' => $store->getGroupId(),
                'view_id' => $magento_store_id,
                )
            );
            $payload['key'] = Mage::getStoreConfig('remarkety/mgconnector/api_key');

            $client = new Zend_Http_Client(
                self::REMARKETY_STOREID_URI,
                $this->_getRequestConfig()
            );
            $client->setParameterPost($payload);
            $response = $client->request(self::REMARKETY_METHOD);

            Mage::log(var_export($payload, true), null, 'remarkety-ext.log');
            Mage::log($response->getStatus(), null, 'remarkety-ext.log');
            Mage::log($response->getBody(), null, 'remarkety-ext.log');

            $body = (array)json_decode($response->getBody());

            Mage::getSingleton('core/session')->setRemarketyLastResponseStatus($response->getStatus() === 200 ? 1 : 0);
            Mage::getSingleton('core/session')->setRemarketyLastResponseMessage(serialize($body));
            if($response->getStatus() == "200"){
                if(!empty($body['storePublicId'])){
                    return $body['storePublicId'];
                }

                //if no store id
                throw new Exception('Response from Remarkety without storeId');
            }

            switch ($response->getStatus()) {
                case '200':
                    return $body;
                case '400':
                    throw new Exception('Request failed. ' . $body['message']);
                default:
                    throw new Exception('Request to remarkety servers failed ('.$response->getStatus().')');
            }
        } catch(Exception $e) {
            Mage::log($e->getMessage(), null, 'remarkety-ext.log');
            throw new Mage_Core_Exception($e->getMessage());
        }
    }
    public function makeRequest($payload)
    {
        try {
            $payload = array_merge($payload, $this->_getPayloadBase());
            $client = new Zend_Http_Client(
                self::REMARKETY_URI,
                $this->_getRequestConfig()
            );
            $client->setParameterPost($payload);
            $response = $client->request(self::REMARKETY_METHOD);

            Mage::log(var_export($payload, true), null, 'remarkety-ext.log');
            Mage::log($response->getStatus(), null, 'remarkety-ext.log');
            Mage::log($response->getBody(), null, 'remarkety-ext.log');

            $body = (array)json_decode($response->getBody());

            Mage::getSingleton('core/session')->setRemarketyLastResponseStatus($response->getStatus() === 200 ? 1 : 0);
            Mage::getSingleton('core/session')->setRemarketyLastResponseMessage(serialize($body));

            switch ($response->getStatus()) {
                case '200':
                    return $body;
                case '400':
                    throw new Exception('Request failed. ' . $body['message']);
                default:
                    throw new Exception('Request to remarkety servers failed ('.$response->getStatus().')');
            }
        } catch(Exception $e) {
            Mage::log($e->getMessage(), null, 'remarkety-ext.log');
            throw new Mage_Core_Exception($e->getMessage());
        }
    }
}
