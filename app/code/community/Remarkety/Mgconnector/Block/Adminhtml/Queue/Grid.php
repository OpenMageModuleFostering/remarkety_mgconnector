<?php

/**
 * Adminhtml queue grid block
 *
 * @category   Remarkety
 * @package    Remarkety_Mgconnector
 * @author     Piotr Pierzak <piotrek.pierzak@gmail.com>
 */
class Remarkety_Mgconnector_Block_Adminhtml_Queue_Grid extends Mage_Adminhtml_Block_Widget_Grid
{
    /**
     * Prepare block
     */
    public function __construct()
    {
        parent::__construct();

        $this
            ->setId('queue_grid')
            ->setDefaultSort('next_attempt')
            ->setDefaultDir('ASC')
            ->setSaveParametersInSession(true)
            ->setUseAjax(true);
    }

    /**
     * Prepare collection
     *
     * @return Remarkety_Mgconnector_Block_Adminhtml_Queue_Grid
     */
    protected function _prepareCollection()
    {
        $collection = Mage::getResourceModel('mgconnector/queue_collection');
        $this->setCollection($collection);

        return parent::_prepareCollection();
    }

    /**
     * Prepare columns
     *
     * @return Remarkety_Mgconnector_Block_Adminhtml_Queue_Grid
     * @throws Exception
     */
    protected function _prepareColumns()
    {
        $helper = Mage::helper('mgconnector');

        $this->addColumn(
            'queue_id', array(
            'header' => $helper->__('Queue #'),
            'width' => '50px',
            'type' => 'number',
            'index' => 'queue_id'
            )
        );
        $this->addColumn(
            'event_type', array(
            'header' => $helper->__('Event Type'),
            'index' => 'event_type',
            'renderer' => 'Remarkety_Mgconnector_Block_Adminhtml_Queue_Grid_Column_Renderer_EventType',
            )
        );
        $this->addColumn(
            'status', array(
            'header' => $helper->__('Status'),
            'width' => '200px',
            'renderer' => 'Remarkety_Mgconnector_Block_Adminhtml_Queue_Grid_Column_Renderer_Status',
            'index' => 'status'
            )
        );
        $this->addColumn(
            'attempts', array(
            'header' => $helper->__('Attempts'),
            'index' => 'attempts'
            )
        );
        $this->addColumn(
            'last_attempt', array(
            'header' => $helper->__('Last Attempt'),
            'width' => '200px',
            'type' => 'datetime',
            'index' => 'last_attempt'
            )
        );
        $this->addColumn(
            'next_attempt', array(
            'header' => $helper->__('Next Attempt'),
            'width' => '200px',
            'type' => 'datetime',
            'index' => 'next_attempt'
            )
        );

        return parent::_prepareColumns();
    }

    /**
     * Return grid url
     *
     * @return string
     */
    public function getGridUrl()
    {
        return $this->getUrl('*/*/grid', array('_current' => true));
    }


    protected function _prepareMassaction()
    {
        $this->setMassactionIdField('queue_id');
        $this->getMassactionBlock()->setFormFieldName('queue');
    
        $this->getMassactionBlock()->addItem(
            'resend', array(
                'label'        => $this->__('Resend'),
                'url'          => $this->getUrl('*/*/massResend')
            )
        );
    
        $this->getMassactionBlock()->addItem(
            'delete', array(
                'label'        => $this->__('Delete'),
                'url'          => $this->getUrl('*/*/massDelete'),
                'confirm'        => $this->__('Really delete all these events?')
            )
        );
    
        return $this;
    }
}
