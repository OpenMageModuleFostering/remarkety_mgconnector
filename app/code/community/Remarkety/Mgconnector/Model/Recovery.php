<?php

/**
 * Remarkety Mgconnector recovery controller.
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Model_Recovery
{
    private $_apikey;

    /**
     * Remarkety_Mgconnector_Model_Recovery constructor.
     */
    public function __construct()
    {
        $this->_apikey = Mage::getStoreConfig('remarkety/mgconnector/api_key');
    }

    /**
     * Restore quote.
     *
     * @param   int $quoteId
     * @return  Remarkety_Mgconnector_Model_Recovery
     * @throws  Mage_Core_Exception
     */
    public function quoteRestore($quoteId)
    {
        /** @var Mage_Checkout_Model_Session $checkoutSession */
        $checkoutSession = Mage::getSingleton('checkout/session');

        $checkoutSession->setQuoteId($quoteId);

        /** @var Mage_Sales_Model_Quote $quoteModel */
        $quote = $checkoutSession->getQuote();

        $quote->getAddressesCollection();
        $quote->getItemsCollection();
        $quote->getPaymentsCollection();

        $quote->getPaymentsCollection()->walk('delete');
        $quote->getAddressesCollection()->walk('delete');

        $quote
            ->setIsActive(true)
            ->setCustomerId(null)
            ->setCustomerEmail(null)
            ->setCustomerFirstname(null)
            ->setCustomerMiddlename(null)
            ->setCustomerLastname(null)
            ->setCustomerGroupId(Mage_Customer_Model_Group::NOT_LOGGED_IN_ID)
            ->save();

        return $this;
    }

    /**
     * Generates signed cart id for urls
     * @param $id
     * @return string
     */
    public function encodeQuoteId($id)
    {
        $sign = md5($id . '.' . $this->_apikey);
        return base64_encode($id . ':' . $sign);
    }

    /**
     * Decodes signed cart ids from urls
     * @param $hashed_id
     * @return bool|int
     */
    public function decodeQuoteId($hashed_id)
    {
        $id = null;
        $parts = base64_decode($hashed_id);
        if(!empty($parts)){
            $split = explode(':', $parts);
            if(count($split) == 2){
                $cart_id = $split[0];
                if(is_numeric($cart_id)){
                    $sign = md5($cart_id . '.' . $this->_apikey);
                    $sign_from_request = $split[1];
                    if($sign === $sign_from_request){
                        return (int)$cart_id;
                    }
                }
            }
        }

        return false;
    }
}
