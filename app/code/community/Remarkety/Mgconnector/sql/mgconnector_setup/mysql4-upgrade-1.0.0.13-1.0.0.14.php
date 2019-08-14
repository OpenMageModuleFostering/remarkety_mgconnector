<?php

/**
 * Upgrade script from version 1.0.0.13 to 1.0.0.14
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
$installer = $this;
$installer->startSetup();

$installer->run(
    "
CREATE TABLE IF NOT EXISTS `{$installer->getTable('mgconnector/queue')}` (
  `queue_id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Remarkety queue ID',
  `event_type` varchar(20) NOT NULL COMMENT 'Event_type',
  `payload` text NOT NULL COMMENT 'Payload',
  `attempts` smallint(5) unsigned NOT NULL DEFAULT '1' COMMENT 'Attempts',
  `last_attempt` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last_attempt',
  `next_attempt` timestamp NULL DEFAULT NULL COMMENT 'Next_attempt',
  `status` smallint(6) NOT NULL DEFAULT '1' COMMENT 'Status',
  PRIMARY KEY (`queue_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Remarkety queue table';
"
);

$installer->endSetup();
