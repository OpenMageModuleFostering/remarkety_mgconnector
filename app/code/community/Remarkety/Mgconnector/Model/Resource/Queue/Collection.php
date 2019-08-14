<?php

/**
 * Mgconnector collection resource model
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
if (version_compare(Mage::getVersion(), '1.6', '>=')) {

    class Remarkety_Mgconnector_Model_Resource_Queue_Collection extends Mage_Core_Model_Resource_Db_Collection_Abstract
    {
        protected function _construct()
        {
            $this->_init('mgconnector/queue');
        }
    }

} else {

    class Remarkety_Mgconnector_Model_Resource_Queue_Collection extends Mage_Core_Model_Mysql4_Collection_Abstract
    {
        protected function _construct()
        {
            $this->_init('mgconnector/queue');
        }
    }

}