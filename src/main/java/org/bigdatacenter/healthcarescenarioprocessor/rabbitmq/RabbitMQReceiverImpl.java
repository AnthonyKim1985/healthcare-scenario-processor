package org.bigdatacenter.healthcarescenarioprocessor.rabbitmq;

import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.TableCreationTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.ScenarioQuery;
import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.ScenarioTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.WorkFlowRequest;
import org.bigdatacenter.healthcarescenarioprocessor.resolver.script.ShellScriptResolver;
import org.bigdatacenter.healthcarescenarioprocessor.service.RawDataDBService;
import org.bigdatacenter.healthcarescenarioprocessor.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQReceiverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private static final Pattern headerPattern = Pattern.compile("(?<=SELECT[ ])[\\w,]+(?=[ ]FROM)");
    private static final Pattern dbAndTableNamePattern = Pattern.compile("(?<=TABLE[ ])\\w+[.]\\w+(?=[ ]STORED)");

    private final ShellScriptResolver shellScriptResolver;

    private final RawDataDBService rawDataDBService;

    @Value("${shellscript.path.home}")
    private String homePath;

    @Autowired
    public RabbitMQReceiverImpl(ShellScriptResolver shellScriptResolver, RawDataDBService rawDataDBService) {
        this.shellScriptResolver = shellScriptResolver;
        this.rawDataDBService = rawDataDBService;
    }

    @Override
    public void runReceiver(WorkFlowRequest workFlowRequest) {
        final Integer dataSetUID = workFlowRequest.getDataSetUID();
        final ScenarioTask scenarioTask = workFlowRequest.getScenarioTask();
        final List<ScenarioQuery> scenarioQueryList = scenarioTask.getScenarioQueryList();

        try {
            for (ScenarioQuery scenarioQuery : scenarioQueryList) {
                logger.info(String.format("%s - %s", currentThreadName, scenarioQuery));

                final String query = scenarioQuery.getQuery();
                switch (scenarioQuery.getType()) {
                    case "creation":
                        final TableCreationTask tableCreationTask = new TableCreationTask(CommonUtil.getHashedString(query), query);
                        rawDataDBService.createTable(tableCreationTask);
                        break;
                    case "extraction":
                        final String dbAndTableName = getDbAndTableName(query);
                        final String dataFileName = dbAndTableName.split("[.]")[1];
                        final String hdfsLocation = CommonUtil.getHdfsLocation(dbAndTableName, dataSetUID);
                        final String header = getHeader(query);

                        final DataExtractionTask dataExtractionTask = new DataExtractionTask(dataFileName, hdfsLocation, query, header);

                        rawDataDBService.extractData(dataExtractionTask);
                        shellScriptResolver.runReducePartsMerger(dataSetUID, hdfsLocation, header, homePath, dataFileName, "workflow");
                        break;
                }
            }
        } catch (Exception e) {
            logger.error(String.format("%s - %s", currentThreadName, e.getMessage()));
            e.printStackTrace();
        }
    }

    private String getHeader(String query) {
        final Matcher matcher = headerPattern.matcher(query);

        String header = null;
        if (matcher.find())
            header = matcher.group();

        if (header == null)
            throw new NullPointerException("The header is null.");

        return header;
    }

    private String getDbAndTableName(String query) {
        final Matcher matcher = dbAndTableNamePattern.matcher(query);

        String dbAndTableName = null;
        if (matcher.find())
            dbAndTableName = matcher.group();

        if (dbAndTableName == null)
            throw new NullPointerException("The dbAndTableName is null.");

        return dbAndTableName;
    }
}