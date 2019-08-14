<?php

/**
 * Upgrade script from version 1.1.2.9 to 1.1.3.0
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Rafał Andryanczyk <rafal.andryanczyk@gmail.com>
 */
$installer = $this;
$installer->startSetup();

$installer->getConnection()
    ->addColumn(
        $installer->getTable('catalogrule'), 'updated_at', array(
        'type'      => Varien_Db_Ddl_Table::TYPE_TIMESTAMP,
        'default'  => Varien_Db_Ddl_Table::TIMESTAMP_UPDATE,
        'comment'  => 'update date',
        )
    );

$installer->endSetup();
