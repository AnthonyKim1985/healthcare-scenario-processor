package org.bigdatacenter.healthcarescenarioprocessor.rabbitmq;

import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.ScenarioTask;

public interface RabbitMQReceiver {
    void runReceiver(ScenarioTask scenarioTask);
}