<?php

/**
 * Remarkety Mgconnector recovery controller.
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_RecoveryController extends Mage_Core_Controller_Front_Action
{
    const MESSAGE_ERROR_WRONG_QUOTE_ID = 'Quote identifier has been not passed or it does not exists.';
    const MESSAGE_ERROR_DURING_PROCESSING = 'During quote recovery error has occurred.';

    /**
     * Cart recovery controller action.
     */
    public function cartAction()
    {
        /** @var Mage_Core_Controller_Request_Http $requestModel */
        $requestModel = $this->getRequest();

        /** @var string|bool $hashedQuoteId */
        $hashedQuoteId = $requestModel->getParam('quote_id', false);

        if (!$hashedQuoteId) {
            Mage::throwException(self::MESSAGE_ERROR_WRONG_QUOTE_ID);
        }

        /** @var Remarkety_Mgconnector_Model_Recovery $recovery */
        $recovery = Mage::getModel('mgconnector/recovery');

        $quoteId = $recovery->decodeQuoteId($hashedQuoteId);
        if (!is_int($quoteId)) {
            Mage::throwException(self::MESSAGE_ERROR_WRONG_QUOTE_ID);
        }

        /** @var Mage_Sales_Model_Quote $quoteModel */
        $quote = Mage::getModel('sales/quote')->load($quoteId);
        if (!$quote->getId()) {
            Mage::throwException(self::MESSAGE_ERROR_WRONG_QUOTE_ID);
        }

        try {
            $recovery->quoteRestore($quoteId);
        } catch (Mage_Core_Exception $e) {
            Mage::throwException(self::MESSAGE_ERROR_DURING_PROCESSING);
        }

        $this->_redirect('checkout/cart');
    }
}
