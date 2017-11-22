package org.bigdatacenter.healthcarescenarioprocessor.rabbitmq;

import org.bigdatacenter.healthcarescenarioprocessor.api.caller.DataIntegrationPlatformAPICaller;
import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.TableCreationTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.transaction.TrRequestInfo;
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

import java.sql.Timestamp;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQReceiverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private static final Pattern headerPattern = Pattern.compile("(?<=SELECT[ ])[\\w,]+(?=[ ]FROM)");
    private static final Pattern dbAndTableNamePattern = Pattern.compile("(?<=TABLE[ ])\\w+[.]\\w+(?=[ ]STORED)");

    private static final String DATASET_NAME = "workflow";

    private final ShellScriptResolver shellScriptResolver;

    private final DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller;

    private final RawDataDBService rawDataDBService;

    @Value("${shellscript.path.home}")
    private String homePath;

    @Autowired
    public RabbitMQReceiverImpl(ShellScriptResolver shellScriptResolver, DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller, RawDataDBService rawDataDBService) {
        this.shellScriptResolver = shellScriptResolver;
        this.dataIntegrationPlatformAPICaller = dataIntegrationPlatformAPICaller;
        this.rawDataDBService = rawDataDBService;
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

    @Override
    public void runReceiver(WorkFlowRequest workFlowRequest) {
        final Integer dataSetUID = workFlowRequest.getRequestInfo().getDataSetUID();

        try {
            final Long jobStartTime = System.currentTimeMillis();
            dataIntegrationPlatformAPICaller.callUpdateJobStartTime(dataSetUID, jobStartTime);
            dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_PROCESSING);

            //
            // TODO: Tasks for Query
            //
            runQueryTask(workFlowRequest);

            //
            // TODO: Tasks for Archiving Files
            //
            runArchiveTask(workFlowRequest);

            final Long jobEndTime = System.currentTimeMillis();
            dataIntegrationPlatformAPICaller.callUpdateJobEndTime(dataSetUID, jobEndTime);
            dataIntegrationPlatformAPICaller.callUpdateElapsedTime(dataSetUID, (jobEndTime - jobStartTime));
            dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_COMPLETED);
        } catch (Exception receiverException) {
            try {
                dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_REJECTED);
                logger.error(String.format("(dataSetUID=%d / threadName=%s) - Exception occurs in RabbitMQReceiver: %s", dataSetUID, currentThreadName, receiverException.getMessage()));
                logger.error(String.format("(dataSetUID=%d / threadName=%s) - Bad Work Flow Request: %s", dataSetUID, currentThreadName, workFlowRequest));
                receiverException.printStackTrace();
            } catch (Exception platformApiException) {
                platformApiException.printStackTrace();
            }
        }
    }

    private void runQueryTask(WorkFlowRequest workFlowRequest) {
        try {
            final TrRequestInfo requestInfo = workFlowRequest.getRequestInfo();
            final Integer dataSetUID = requestInfo.getDataSetUID();
            final ScenarioTask scenarioTask = workFlowRequest.getScenarioTask();
            final List<ScenarioQuery> scenarioQueryList = scenarioTask.getScenarioQueryList();

            for (int i = 0, scenarioQueryListSize = scenarioQueryList.size(); i < scenarioQueryListSize; i++) {
                final ScenarioQuery scenarioQuery = scenarioQueryList.get(i);
                final Long queryBeginTime = System.currentTimeMillis();
                logger.info(String.format("(dataSetUID=%d / threadName=%s) - Processing %d/%d query.", dataSetUID, currentThreadName, (i + 1), scenarioQueryListSize));

                final String query = scenarioQuery.getQuery();
                switch (scenarioQuery.getType()) {
                    case "creation":
                        logger.info(String.format("(dataSetUID=%d / threadName=%s) - Start table creation at Hive Query: %s", dataSetUID, currentThreadName, scenarioQuery.getQuery()));

                        final TableCreationTask tableCreationTask = new TableCreationTask(CommonUtil.getHashedString(query), query);
                        rawDataDBService.createTable(tableCreationTask);
                        break;
                    case "extraction":
                        logger.info(String.format("(dataSetUID=%d / threadName=%s) - Start data extraction at Hive Query: %s", dataSetUID, currentThreadName, scenarioQuery.getQuery()));

                        final String dbAndTableName = getDbAndTableName(query);
                        final String dataFileName = dbAndTableName.split("[.]")[1];
                        final String hdfsLocation = CommonUtil.getHdfsLocation(dbAndTableName, dataSetUID);
                        final String header = getHeader(query);

                        final DataExtractionTask dataExtractionTask = new DataExtractionTask(dataFileName, hdfsLocation, query, header);

                        rawDataDBService.extractData(dataExtractionTask);
                        shellScriptResolver.runReducePartsMerger(dataSetUID, hdfsLocation, header, homePath, dataFileName, DATASET_NAME);
                        break;
                }

                final Long queryEndTime = System.currentTimeMillis() - queryBeginTime;
                logger.info(String.format("(dataSetUID=%d / threadName=%s) - Finish Hive Query: %s, Elapsed time: %d ms", dataSetUID, currentThreadName, scenarioQuery, queryEndTime));
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private void runArchiveTask(WorkFlowRequest workFlowRequest) {
        try {
            final TrRequestInfo requestInfo = workFlowRequest.getRequestInfo();
            final Integer dataSetUID = requestInfo.getDataSetUID();

            //
            // TODO: Archive the extracted data set and finally send the file to FTP server.
            //
            final String archiveFileName = String.format("%s_%s.tar.gz", requestInfo.getUserID(), String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
            final String ftpLocation = String.format("/%s/%s", requestInfo.getUserID(), DATASET_NAME);

            final long archiveFileBeginTime = System.currentTimeMillis();
            logger.info(String.format("(dataSetUID=%d / threadName=%s) - Start archiving the extracted data set: %s", dataSetUID, currentThreadName, archiveFileName));
            shellScriptResolver.runArchiveExtractedDataSet(dataSetUID, archiveFileName, ftpLocation, homePath, DATASET_NAME);
            logger.info(String.format("(dataSetUID=%d / threadName=%s) - Finish archiving the extracted data set: %s, Elapsed time: %d ms", dataSetUID, currentThreadName, archiveFileName, (System.currentTimeMillis() - archiveFileBeginTime)));

            final String ftpURI = String.format("%s/%s", ftpLocation, archiveFileName);
            dataIntegrationPlatformAPICaller.callCreateFtpInfo(requestInfo.getDataSetUID(), requestInfo.getUserID(), ftpURI);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}