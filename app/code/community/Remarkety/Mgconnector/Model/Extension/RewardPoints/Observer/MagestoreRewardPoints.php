<?php

/**
 * Magestore reward points observer class.
 *
 * @category Remarkety
 * @package  Remarkety_Mgconnector
 * @author   Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Model_Extension_RewardPoints_Observer_MagestoreRewardPoints
    extends Remarkety_Mgconnector_Model_Extension_RewardPoints_Observer_AbstractObserver
{
    /**
     * Check if magestore reward points extension exists and is enabled.
     * If yes on each customer reward points update customer update event
     * will be triggered to remarkety api.
     *
     * @param Varien_Object $observer Observer data.
     *
     * @return Remarkety_Mgconnector_Model_Extension_RewardPoints_Observer_MagestoreRewardPoints
     */
    public function updateCustomerRewardPoints(Varien_Object $observer)
    {
        $rewardPointsModel = Mage::getModel(
            'mgconnector/extension_rewardPoints_magestoreRewardPoints'
        );
        if ($rewardPointsModel->isEnabled() === false) {
            return $this;
        }

        $transactionModel = $observer->getEvent()->getDataObject();
        $transactionModel = Mage::getModel('rewardpoints/transaction')
            ->load($transactionModel->getId());
        if ((int)$transactionModel->getStatus() !== Magestore_RewardPoints_Model_Transaction::STATUS_COMPLETED) {
            return $this;
        }

        $rewardPointsCustomerModel = Mage::getModel('rewardpoints/customer')
            ->load($transactionModel->getCustomerId(), 'customer_id');

        $rewardPointsData = array(
            'points' => (int)$rewardPointsCustomerModel->getPointBalance(),
            'spent' => (int)$rewardPointsCustomerModel->getSpentBalance(),
            'hold' => (int)$rewardPointsCustomerModel->getHoldingBalance(),
            'transaction' => array(
                'created_at' => $transactionModel->getCreatedTime(),
                'points' => (int)$transactionModel->getPointAmount(),
                'store_id' => (int)$transactionModel->getStoreId(),
                'action' => $transactionModel->getAction(),
                'expiration_date' => $transactionModel->getExpirationDate(),
            ),
        );

        $this->_customer = Mage::getModel('customer/customer')
            ->load($transactionModel->getCustomerId());
        $this->sendCustomerUpdateRequest($rewardPointsData);

        return $this;
    }
}
