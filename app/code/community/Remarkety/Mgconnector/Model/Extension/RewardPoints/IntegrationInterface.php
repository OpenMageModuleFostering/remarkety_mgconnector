<?php

/**
 * Reward points integration interface.
 *
 * @category Remarkety
 * @package  Remarkety_Mgconnector
 * @author   Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
interface Remarkety_Mgconnector_Model_Extension_RewardPoints_IntegrationInterface
{
    /**
     * Method should return bool value whenever extension is enabled or not.
     * It should check if module is enabled and if extension has its own
     * enable/disable flag it should be checked as well.
     *
     * @return bool
     */
    public function isEnabled();

    /**
     * Method should return extension like, example: Remarkety_Mgconnector.
     *
     * @return string
     */
    public function getModuleName();

    /**
     * Method should return information about customer reward points
     * balance which will be used in customer update events.
     *
     * @param int $customerId Customer id.
     *
     * @return array
     */
    public function getCustomerUpdateData($customerId);

    /**
     * Modifies the collection to include the customer points
     * @param Mage_Customer_Model_Resource_Customer_Collection $collection
     * @return mixed
     */
    public function modifyCustomersCollection(&$collection);
}
