<?php

/**
 * Reward points abstract observer class.
 *
 * @category Remarkety
 * @package  Remarkety_Mgconnector
 * @author   Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
abstract class Remarkety_Mgconnector_Model_Extension_RewardPoints_Observer_AbstractObserver
    extends Remarkety_Mgconnector_Model_Observer
{
    /**
     * Send customer update request to remarkety containing
     * information about reward points update.
     *
     * Input data array should have structure:
     * array(
     *     'points' => int value of current points balance,
     *     'hold' => int value of points which are on hold,
     *     'spent' => int value of points which customer spent,
     *     'transaction' => array(
     *         'action' => string type of action
     *     ),
     * )
     *
     * @param array $rewardPointsData Reward points data.
     *
     * @return Remarkety_Mgconnector_Model_Extension_RewardPoints_Observer_AbstractObserver
     */
    public function sendCustomerUpdateRequest(array $rewardPointsData)
    {
        $customerData = $this->_prepareCustomerUpdateData();
        $customerData['rewards'] = $rewardPointsData;

        //queue customer update event
        $this->_queueRequest(
            'customers/update',
            $customerData,
            1,
            null
        );

        return $this;
    }
}
