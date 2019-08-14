<?php

/**
 * Observer model, which handle few events and send post request
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */

if (!defined("REMARKETY_LOG"))
	define('REMARKETY_LOG', 'remarkety_mgconnector.log');

class Remarkety_Mgconnector_Model_Observer
{
    const REMARKETY_EVENTS_ENDPOINT = 'https://api-events.remarkety.com/v1';
    const REMARKETY_METHOD = 'POST';
    const REMARKETY_TIMEOUT = 2;
    const REMARKETY_VERSION = 0.9;
    const REMARKETY_PLATFORM = 'MAGENTO';

    protected $_token = null;
    protected $_intervals = null;
    protected $_customer = null;
    protected $_hasDataChanged = false;

    protected $_subscriber = null;
    protected $_origSubsciberData = null;

    protected $_address = null;
    protected $_origAddressData = null;

    public function __construct()
    {
        $this->_token = Mage::getStoreConfig('remarkety/mgconnector/api_key');
        $intervals = Mage::getStoreConfig('remarkety/mgconnector/intervals');
        $this->_intervals = explode(',', $intervals);
    }



    public function triggerCustomerAddressBeforeUpdate($observer)
    {
    	$address = Mage::getSingleton('customer/session')->getCustomer()->getDefaultBillingAddress();
        if(!empty($address)) {
            $this->_origAddressData = $address->getData();
        }
        return $this;
    }

    public function beforeBlockToHtml(Varien_Event_Observer $observer) {

        $grid = $observer->getBlock();

        /**
         * Mage_Adminhtml_Block_Customer_Grid
         */
        if ($grid instanceof Mage_Adminhtml_Block_Promo_Quote_Edit_Tab_Coupons_Grid) {
            $grid->addColumnAfter(
                'expiration_date',
                array(
                    'header' => Mage::helper('salesrule')->__('Expiration date'),
                    'index' => 'expiration_date',
                    'type'   => 'datetime',
                    'default'   => '-',
                    'align'  => 'center',
                    'width'  => '160'
                ),
                'created_at'
            );

            $yesnoOptions = array(null => 'No','1' => 'Yes','' => 'No');

            $grid->addColumnAfter('added_by_remarkety', array(
                'header'    => Mage::helper('salesrule')->__('Created By Remarkety'),
                'index'     => 'added_by_remarkety',
                'type'      => 'options',
                'options'   => $yesnoOptions,
                'width'  => '30',
                'align'  => 'center',
            ),'expiration_date');

        }

    }

    public function triggerCustomerAddressUpdate($observer)
    {
        $this->_address = $observer->getEvent()->getCustomerAddress();
        $this->_customer = $this->_address->getCustomer();

        if(Mage::registry('remarkety_customer_save_observer_executed_'.$this->_customer->getId())) {
            return $this;
        }

        $isDefaultBilling =
        	($this->_customer == null || $this->_customer->getDefaultBillingAddress() == null)
        	? false
        	: ($this->_address->getId() == $this->_customer->getDefaultBillingAddress()->getId());
        if (!$isDefaultBilling || !$this->_customer->getId()) {
        	return $this;
        }

        $this->_customerUpdate();

        Mage::register('remarkety_customer_save_observer_executed_'.$this->_customer->getId(),true);
        return $this;
    }

    public function triggerCustomerUpdate($observer)
    {
        $this->_customer = $observer->getEvent()->getCustomer();

        if(Mage::registry('remarkety_customer_save_observer_executed_'.$this->_customer->getId()) || !$this->_customer->getId()) {
            return $this;
        }

        if($this->_customer->getOrigData() === null) {
            $this->_customerRegistration();
        } else {
            $this->_customerUpdate();
        }

        Mage::register('remarkety_customer_save_observer_executed_'.$this->_customer->getId(),true);
        return $this;
    }

    public function triggerSubscribeUpdate($observer)
    {
        $this->_subscriber = $observer->getEvent()->getSubscriber();

        $loggedIn = Mage::getSingleton('customer/session')->isLoggedIn();

        if($this->_subscriber->getId() && !$loggedIn) {
            if($this->_subscriber->getCustomerId() && Mage::registry('remarkety_customer_save_observer_executed_'.$this->_subscriber->getCustomerId())) {
                return $this;
            }
            // Avoid loops - If this unsubsribe was triggered by remarkety, no need to update us
            if (Mage::registry('remarkety_subscriber_deleted'))
            	return $this;
            $this->makeRequest('customers/create', $this->_prepareCustomerSubscribtionUpdateData(true));
            $email = $this->_subscriber->getSubscriberEmail();
            if(!empty($email)){

                //save email to cart if needed
                $cart = Mage::getSingleton('checkout/session')->getQuote();
                if($cart && !is_null($cart->getId()) && is_null($cart->getCustomerEmail())){
                    $cart->setCustomerEmail($email)->save();
                }

                Mage::getSingleton('customer/session')->setSubscriberEmail($email);
            }
        }

        return $this;
    }

    public function triggerSubscribeDelete($observer)
    {
        $this->_subscriber = $observer->getEvent()->getSubscriber();
        if(!Mage::registry('remarkety_subscriber_deleted_'.$this->_subscriber->getEmail()) && $this->_subscriber->getId()) {
            $this->makeRequest('customers/update', $this->_prepareCustomerSubscribtionDeleteData());
        }

        return $this;
    }

    public function triggerCustomerDelete($observer)
    {
        $this->_customer = $observer->getEvent()->getCustomer();
        if (!$this->_customer->getId()) {
            return $this;
        }

        $this->makeRequest('customers/delete', array(
            'id' => (int)$this->_customer->getId(),
            'email' => $this->_customer->getEmail(),
        ));
        return $this;
    }

    public function triggerProductSave($observer) {
        // TODO - Need to implement
        return $this;
    }

    protected function _customerRegistration()
    {
        $this->makeRequest('customers/create', $this->_prepareCustomerUpdateData());
        return $this;
    }

    protected function _customerUpdate()
    {
        if($this->_hasDataChanged()) {
            $this->makeRequest('customers/update', $this->_prepareCustomerUpdateData());
        }
        return $this;
    }

    protected function _hasDataChanged()
    {
        if(!$this->_hasDataChanged && $this->_customer) {
            $validate = array(
                'firstname',
                'lastname',
                'title',
                'birthday',
                'gender',
                'email',
                'group_id',
                'default_billing',
                'is_subscribed',
            );
	        $originalData = $this->_customer->getOrigData();
	        $currentData = $this->_customer->getData();
	        foreach ($validate as $field) {
				if (isset($originalData[$field])) {
					if (!isset($currentData[$field]) || $currentData[$field] != $originalData[$field]) {
						$this->_hasDataChanged = true;
						break;
					}
				}
			}
// This part has been replaced by the loop above to avoid comparing objects in array_diff
//            $customerDiffKeys = array_keys( array_diff($this->_customer->getData(), $this->_customer->getOrigData()) );
//
//            if(array_intersect($customerDiffKeys, $validate)) {
//                $this->_hasDataChanged = true;
//            }
            $customerData = $this->_customer->getData();
            if(!$this->_hasDataChanged && isset($customerData['is_subscribed'])) {
                $subscriber = Mage::getModel('newsletter/subscriber')->loadByEmail($this->_customer->getEmail());
                $isSubscribed = $subscriber->getId() ? $subscriber->getData('subscriber_status') == Mage_Newsletter_Model_Subscriber::STATUS_SUBSCRIBED : false;

                if($customerData['is_subscribed'] !== $isSubscribed) {
                    $this->_hasDataChanged = true;
                }
            }
        }
        if(!$this->_hasDataChanged && $this->_address && $this->_origAddressData) {
            $validate = array(
                'street',
                'city',
                'region',
                'postcode',
                'country_id',
                'telephone',
            );
            $addressDiffKeys = array_keys( array_diff($this->_address->getData(), $this->_origAddressData) );

            if(array_intersect($addressDiffKeys, $validate)) {
                $this->_hasDataChanged = true;
            }
        }

        return $this->_hasDataChanged;
    }

    protected function _getRequestConfig($eventType)
    {
        return array(
            'adapter' => 'Zend_Http_Client_Adapter_Curl',
            'curloptions' => array(
//                CURLOPT_FOLLOWLOCATION => true,
                CURLOPT_HEADER => true,
                CURLOPT_CONNECTTIMEOUT => self::REMARKETY_TIMEOUT
//	            CURLOPT_SSL_CIPHER_LIST => "RC4-SHA"
            ),
        );
    }

    protected function _getHeaders($eventType,$payload)
    {
        $domain = Mage::getBaseUrl(Mage_Core_Model_Store::URL_TYPE_WEB);
        $domain = substr($domain, 7, -1);

        $headers = array(
            'X-Domain: ' . $domain,
            'X-Token: ' . $this->_token,
            'X-Event-Type: ' . $eventType,
            'X-Platform: ' . self::REMARKETY_PLATFORM,
            'X-Version: ' . self::REMARKETY_VERSION,
        );
        if (isset($payload['storeId']))
            $headers[] = 'X-Magento-Store-Id: ' . $payload['storeId'];
        return $headers;
    }

    public function makeRequest($eventType, $payload, $attempt = 1, $queueId = null)
    {
        try {
            $client = new Zend_Http_Client(self::REMARKETY_EVENTS_ENDPOINT, $this->_getRequestConfig($eventType));
            $payload = array_merge($payload, $this->_getPayloadBase($eventType));
            $headers = $this->_getHeaders($eventType, $payload);
            unset($payload['storeId']);
            $json = json_encode($payload);

            $response = $client
                ->setHeaders($headers)
                ->setRawData($json, 'application/json')
                ->request(self::REMARKETY_METHOD);
            
            Mage::log("Sent event to endpoint: ".$json."; Response (".$response->getStatus()."): ".$response->getBody(), \Zend_Log::DEBUG, REMARKETY_LOG);
            switch ($response->getStatus()) {
                case '200':
                    return true;
                case '400':
                    throw new Exception('Request has been malformed.');
                case '401':
                    throw new Exception('Request failed, probably wrong API key or inactive account.');
                default:
                    $this->_queueRequest($eventType, $payload, $attempt, $queueId);
            }
        } catch(Exception $e) {
            $this->_queueRequest($eventType, $payload, $attempt, $queueId);
        }

        return false;
    }

    protected function _queueRequest($eventType, $payload, $attempt, $queueId)
    {
        $queueModel = Mage::getModel('mgconnector/queue');

        if(!empty($this->_intervals[$attempt-1])) {
            $now = time();
            $nextAttempt = $now + (int)$this->_intervals[$attempt-1] * 60;
            if($queueId) {
                $queueModel->load($queueId);
                $queueModel->setAttempts($attempt);
                $queueModel->setLastAttempt( date("Y-m-d H:i:s", $now) );
                $queueModel->setNextAttempt( date("Y-m-d H:i:s", $nextAttempt) );
            } else {
                $queueModel->setData(array(
                    'event_type' => $eventType,
                    'payload' => serialize($payload),
                    'attempts' => $attempt,
                    'last_attempt' => date("Y-m-d H:i:s", $now),
                    'next_attempt' => date("Y-m-d H:i:s", $nextAttempt),
                ));
            }
            return $queueModel->save();
        } elseif($queueId) {
            $queueModel->load($queueId);
            $queueModel->setStatus(0);
            return $queueModel->save();
        }
        return false;
    }

    protected function _getPayloadBase($eventType)
    {
        date_default_timezone_set('UTC');
        $arr = array(
            'timestamp' => (string)time(),
            'event_id' => $eventType,
        );
        return $arr;
    }

    protected function _prepareCustomerUpdateData()
    {
        $arr = array(
            'id' => (int)$this->_customer->getId(),
            'email' => $this->_customer->getEmail(),
            'created_at' => date('c', strtotime($this->_customer->getCreatedAt())),
            'first_name' => $this->_customer->getFirstname(),
            'last_name' => $this->_customer->getLastname(),
            'store_id'  => $this->_customer->getStoreId(),
            //'extra_info' => array(),
        );

        $isSubscribed = $this->_customer->getIsSubscribed();
        if($isSubscribed === null) {
            $subscriber = Mage::getModel('newsletter/subscriber')->loadByEmail($this->_customer->getEmail());
            if($subscriber->getId()) {
                $isSubscribed = $subscriber->getData('subscriber_status') == Mage_Newsletter_Model_Subscriber::STATUS_SUBSCRIBED;
            } else {
                $isSubscribed = false;
            }
        }
        $arr = array_merge($arr, array('accepts_marketing' => (bool)$isSubscribed));

        if($title = $this->_customer->getPrefix()) {
            $arr = array_merge($arr, array('title' => $title));
        }

        if($dob = $this->_customer->getDob()) {
            $arr = array_merge($arr, array('birthdate' => $dob));
        }

        if($gender = $this->_customer->getGender()) {
            $arr = array_merge($arr, array('gender' => $gender));
        }

        if($address = $this->_customer->getDefaultBillingAddress()) {
            $street = $address->getStreet();
            $arr = array_merge($arr, array('default_address' => array(
                'address1' => isset($street[0]) ? $street[0] : '',
                'address2' => isset($street[1]) ? $street[1] : '',
                'city' => $address->getCity(),
                'province' => $address->getRegion(),
                'phone' => $address->getTelephone(),
                'country_code' => $address->getCountryId(),
                'zip' => $address->getPostcode(),
            )));
        }

        $tags = $this->_getCustomerProductTags();
        if(!empty($tags) && $tags->getSize()) {
            $tagsArr = array();
            foreach ($tags as $_tag) {
                $tagsArr[] = $_tag->getName();
            }
            $arr = array_merge($arr, array('tags' => $tagsArr));
        }

        if($group = Mage::getModel('customer/group')->load($this->_customer->getGroupId())) {
            $arr = array_merge($arr, array('groups' => array(
                array(
                    'id' => (int)$this->_customer->getGroupId(),
                    'name' => $group->getCustomerGroupCode(),
                )
            )));
        }

        return $arr;
    }

    protected function _getCustomerProductTags()
    {
        $tags = Mage::getModel('tag/tag')->getResourceCollection();
        if (!empty($tags)) {
            $tags = $tags
                ->joinRel()
                ->addCustomerFilter($this->_customer->getId());
        }
        return $tags;
    }

    protected function _prepareCustomerSubscribtionUpdateData($newsletter = false)
    {
        $quote = Mage::getSingleton('checkout/session')->getQuote();
        $store = Mage::app()->getStore();

        $arr = array(
            'email' => $this->_subscriber->getSubscriberEmail(),
            'accepts_marketing' => $this->_subscriber->getData('subscriber_status') == Mage_Newsletter_Model_Subscriber::STATUS_SUBSCRIBED,
            'storeId' => $store->getStoreId()
        );

        if($newsletter && (!is_object($quote) || $quote->getCheckoutMethod() !== Mage_Sales_Model_Quote::CHECKOUT_METHOD_GUEST) ){
            $arr['is_newsletter_subscriber'] = true;
        }

        return $arr;
    }

    protected function _prepareCustomerSubscribtionDeleteData()
    {
        $store = Mage::app()->getStore();

        $arr = array(
            'email' => $this->_subscriber->getSubscriberEmail(),
            'accepts_marketing' => false,
            'storeId' => $store->getStoreId()
        );

        return $arr;
    }

    public function resend($queueItems,$resetAttempts = false) {
    	$sent=0;
    	foreach($queueItems as $_queue) {
    		$result = $this->makeRequest($_queue->getEventType(), unserialize($_queue->getPayload()), $resetAttempts ? 1 : ($_queue->getAttempts()+1), $_queue->getId());
    		if($result) {
    			Mage::getModel('mgconnector/queue')
    			->load($_queue->getId())
    			->delete();
    			$sent++;
    		}
    	}
    	return $sent;
    }
    
    public function run()
    {
        $collection = Mage::getModel('mgconnector/queue')->getCollection();
        $nextAttempt = date("Y-m-d H:i:s");
        $collection
            ->getSelect()
            ->where('next_attempt <= ?', $nextAttempt)
            ->where('status = 1')
            ->order('main_table.next_attempt asc');
		$this->resend($collection);
        return $this;
    }
}