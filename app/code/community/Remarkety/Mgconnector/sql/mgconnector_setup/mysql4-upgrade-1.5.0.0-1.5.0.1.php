<?php

/**
 * Upgrade script from version 1.5.0.0 to 1.5.0.1
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Bnaya Livne <bnaya@remarkety.com>
 */
$installer = $this;
$installer->startSetup();


$installer->getConnection()
    ->addColumn(
        $installer->getTable('mgconnector/queue'), 'last_error_message', array(
        'type'      => Varien_Db_Ddl_Table::TYPE_TEXT,
        'nullable'  => true,
        'default'  => null,
        'comment'  => 'Last error message',
        )
    );

$installer->endSetup();
