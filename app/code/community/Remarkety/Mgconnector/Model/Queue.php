<?php

/**
 * Queue model
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Model_Queue extends Mage_Core_Model_Abstract
{
    protected function _construct()
    {
        $this->_init('mgconnector/queue');
    }
}
