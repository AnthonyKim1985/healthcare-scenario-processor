package org.bigdatacenter.healthcarescenarioprocessor.rabbitmq;

import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.TableCreationTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.ScenarioQuery;
import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.ScenarioTask;
import org.bigdatacenter.healthcarescenarioprocessor.resolver.script.ShellScriptResolver;
import org.bigdatacenter.healthcarescenarioprocessor.service.RawDataDBService;
import org.bigdatacenter.healthcarescenarioprocessor.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQReceiverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final ShellScriptResolver shellScriptResolver;

    private final RawDataDBService rawDataDBService;

    @Autowired
    public RabbitMQReceiverImpl(ShellScriptResolver shellScriptResolver, RawDataDBService rawDataDBService) {
        this.shellScriptResolver = shellScriptResolver;
        this.rawDataDBService = rawDataDBService;
    }

    @Override
    public void runReceiver(ScenarioTask scenarioTask) {
        final List<ScenarioQuery> scenarioQueryList = scenarioTask.getScenarioQueryList();

        for (ScenarioQuery scenarioQuery : scenarioQueryList) {
            logger.info(String.format("%s - %s", currentThreadName, scenarioQuery));

            final String query = scenarioQuery.getQuery();

            switch (scenarioQuery.getType()) {
                case "extraction":
                    DataExtractionTask dataExtractionTask = new DataExtractionTask("", "", query, "");
                    break;
                case "creation":
                    TableCreationTask tableCreationTask = new TableCreationTask(CommonUtil.getHashedString(query), query);
                    break;
            }
        }
    }
}