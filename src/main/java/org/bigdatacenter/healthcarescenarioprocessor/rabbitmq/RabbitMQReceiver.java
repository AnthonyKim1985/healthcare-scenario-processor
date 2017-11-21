package org.bigdatacenter.healthcarescenarioprocessor.rabbitmq;

import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.WorkFlowRequest;

public interface RabbitMQReceiver {
    void runReceiver(WorkFlowRequest workFlowRequest);
}