<?php

require_once __DIR__ . DS . 'AbstractController.php';

/**
 * Reward points extension controller.
 *
 * @category Remarkety
 * @package  Remarkety_Mgconnector
 * @author   Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Adminhtml_Extension_RewardPointsController
    extends Remarkety_Mgconnector_Adminhtml_Extension_AbstractController
{
    /**
     * Index action.
     *
     * @return void
     */
    public function indexAction()
    {
        $this
            ->initAction()
            ->_title($this->__('Reward Points Extensions'))
            ->renderLayout();
    }
}
