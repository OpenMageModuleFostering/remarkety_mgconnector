<?php

/**
 * Upgrade script from version 1.4.10.4 to 1.5.0.0
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Bnaya Livne <bnaya@remarkety.com>
 */
$installer = $this;
$installer->startSetup();


$installer->getConnection()
    ->addColumn(
        $installer->getTable('mgconnector/queue'), 'store_id', array(
        'type'      => Varien_Db_Ddl_Table::TYPE_NUMERIC,
        'nullable'  => true,
        'default'  => null,
        'comment'  => 'Store view id to use in the requests',
        )
    );

$installer->endSetup();
