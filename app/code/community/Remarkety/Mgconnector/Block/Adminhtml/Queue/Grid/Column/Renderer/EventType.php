<?php

/**
 * Adminhtml queue grid status column renderer block
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Queue_Grid_Column_Renderer_EventType
    extends Mage_Adminhtml_Block_Widget_Grid_Column_Renderer_Abstract
{
    /**
     * Column renderer
     *
     * @param Varien_Object $row
     * @return string
     */
    public function render(Varien_Object $row)
    {
        $value = $row->getData($this->getColumn()->getIndex());
        try {
            $payload = json_encode(unserialize($row->getData('payload')));
        } catch (\Exception $e) {
            $payload = "?";
        }

        return '<span title="'.htmlentities($payload).'">'.$value.'</span>';
    }
}
