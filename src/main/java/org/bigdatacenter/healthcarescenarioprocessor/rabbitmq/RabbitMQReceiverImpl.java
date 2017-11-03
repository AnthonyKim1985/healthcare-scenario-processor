package org.bigdatacenter.healthcarescenarioprocessor.rabbitmq;

import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.ScenarioTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQReceiverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    @Override
    public void runReceiver(ScenarioTask scenarioTask) {

    }
}
