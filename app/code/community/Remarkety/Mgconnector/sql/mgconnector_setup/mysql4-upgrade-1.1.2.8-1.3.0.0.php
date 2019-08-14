<?php

/**
 * Upgrade script from version 1.1.2.8 to 1.3.0.0
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Rafał Andryanczyk <rafal.andryanczyk@gmail.com>
 */
$installer = $this;
$installer->startSetup();

$installer->getConnection()
    ->addColumn(
        $installer->getTable('salesrule/coupon'), 'added_by_remarkety', array(
        'type'      => Varien_Db_Ddl_Table::TYPE_SMALLINT,
        'nullable'  => true,
        'default'  => null,
        'comment'  => '1 = added by remarkety api',
        )
    );

$installer->endSetup();
