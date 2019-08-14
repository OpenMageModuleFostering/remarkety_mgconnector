<?php

/**
 * Magestore reward points integration class.
 *
 * @category Remarkety
 * @package  Remarkety_Mgconnector
 * @author   Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Model_Extension_RewardPoints_MagestoreRewardPoints
    extends Remarkety_Mgconnector_Model_Extension_RewardPoints_AbstractIntegration
{
    /**
     * {@inheritdoc}
     */
    public function isEnabled()
    {
        return (bool)$this->getStoreConfig('rewardpoints/general/enable');
    }

    /**
     * {@inheritdoc}
     */
    public function getModuleName()
    {
        return 'Magestore_RewardPoints';
    }

    /**
     * {@inheritdoc}
     */
    public function getCustomerUpdateData($customerId)
    {
        $rewardPointsCustomerModel = Mage::getModel('rewardpoints/customer')
            ->load($customerId, 'customer_id');

        return array(
            'points' => (int)$rewardPointsCustomerModel->getPointBalance(),
            'spent' => (int)$rewardPointsCustomerModel->getSpentBalance(),
            'hold' => (int)$rewardPointsCustomerModel->getHoldingBalance(),
        );
    }

    /**
     * Modifies the collection to include the customer points
     * @param Mage_Customer_Model_Resource_Customer_Collection $collection
     * @return mixed
     */
    public function modifyCustomersCollection(&$collection)
    {
        $collection->getSelect()
            ->joinLeft(
                array('rp' => $collection->getTable('rewardpoints/customer')),
                'e.entity_id = rp.customer_id',
                array('rewards_points' => 'point_balance')
            );
        return null;
    }
}
