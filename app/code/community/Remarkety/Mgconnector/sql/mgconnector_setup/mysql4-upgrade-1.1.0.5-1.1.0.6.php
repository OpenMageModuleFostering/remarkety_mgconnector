<?php

/**
 * Upgrade script from version 1.1.0.5 to 1.1.0.6
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
$installer = $this;
$installer->startSetup();

//@TODO remove installed entry for 0 scope with api key

$installer->endSetup();
