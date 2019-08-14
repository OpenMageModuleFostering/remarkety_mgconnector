<?php

/**
 * Upgrade script from version 1.4.7.2 to 1.4.8.0
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Rafał Andryanczyk <rafal.andryanczyk@gmail.com>
 */
$installer = $this;
$installer->startSetup();


$installer->getConnection()
    ->addColumn(
        $installer->getTable('catalogrule/rule'), 'updated_at', array(
        'type'      => Varien_Db_Ddl_Table::TYPE_TIMESTAMP,
        'nullable'  => true,
        'default'  => null,
        'comment'  => 'updated_at',
        )
    );

$installer->endSetup();
