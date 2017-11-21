package org.bigdatacenter.healthcarescenarioprocessor.rabbitmq;

import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.ScenarioTask;

import java.util.Map;

public interface RabbitMQReceiver {
    void runReceiver(Map<String, Object> rabbitMQMap);
}