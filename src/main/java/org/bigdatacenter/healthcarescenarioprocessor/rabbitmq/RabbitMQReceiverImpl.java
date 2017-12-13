package org.bigdatacenter.healthcarescenarioprocessor.rabbitmq;

import org.bigdatacenter.healthcarescenarioprocessor.api.caller.DataIntegrationPlatformAPICaller;
import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.TableCreationTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.transaction.TrRequestInfo;
import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.ScenarioQuery;
import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.ScenarioTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.WorkFlowRequest;
import org.bigdatacenter.healthcarescenarioprocessor.resolver.matcher.creation.CreationQueryMatcher;
import org.bigdatacenter.healthcarescenarioprocessor.resolver.matcher.extraction.ExtractionQueryMatcher;
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

@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQReceiverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final ShellScriptResolver shellScriptResolver;

    private final DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller;

    private final RawDataDBService rawDataDBService;

    private final CreationQueryMatcher creationQueryMatcher;

    private final ExtractionQueryMatcher extractionQueryMatcher;

    @Value("${shellscript.path.home}")
    private String homePath;

    @Autowired
    public RabbitMQReceiverImpl(ShellScriptResolver shellScriptResolver,
                                DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller,
                                RawDataDBService rawDataDBService,
                                CreationQueryMatcher creationQueryMatcher,
                                ExtractionQueryMatcher extractionQueryMatcher)
    {
        this.shellScriptResolver = shellScriptResolver;
        this.dataIntegrationPlatformAPICaller = dataIntegrationPlatformAPICaller;
        this.rawDataDBService = rawDataDBService;
        this.creationQueryMatcher = creationQueryMatcher;
        this.extractionQueryMatcher = extractionQueryMatcher;
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

                        final String dbAndTableNameForCreation = creationQueryMatcher.getDbAndTableName(query);
                        final String selectQuery = creationQueryMatcher.getSelectQuery(query);

                        final TableCreationTask tableCreationTask = new TableCreationTask(dbAndTableNameForCreation, selectQuery);
                        rawDataDBService.createTable(tableCreationTask);
                        break;
                    case "extraction":
                        logger.info(String.format("(dataSetUID=%d / threadName=%s) - Start data extraction at Hive Query: %s", dataSetUID, currentThreadName, scenarioQuery.getQuery()));

                        final String dbAndTableNameForExtraction = extractionQueryMatcher.getDbAndTableName(query);
                        final String dataSetName = extractionQueryMatcher.getDbName(query);
                        final String dataFileName = extractionQueryMatcher.getTableName(query);
                        final String hdfsLocation = CommonUtil.getHdfsLocation(dbAndTableNameForExtraction, dataSetUID);
                        final String header = extractionQueryMatcher.getHeader(query);

                        final DataExtractionTask dataExtractionTask = new DataExtractionTask(dataFileName, hdfsLocation, query, header);
                        rawDataDBService.extractData(dataExtractionTask);
                        shellScriptResolver.runReducePartsMerger(dataSetUID, hdfsLocation, header, homePath, dataFileName, dataSetName);
                        break;
                }

                final Long queryEndTime = System.currentTimeMillis() - queryBeginTime;
                logger.info(String.format("(dataSetUID=%d / threadName=%s) - Finish Hive Query: %s, Elapsed time: %d ms", dataSetUID, currentThreadName, scenarioQuery, queryEndTime));
            }
        } catch (Exception e) {
            e.printStackTrace();
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
            final String ftpLocation = String.format("/%s/workflow", requestInfo.getUserID());

            final long archiveFileBeginTime = System.currentTimeMillis();
            logger.info(String.format("(dataSetUID=%d / threadName=%s) - Start archiving the extracted data set: %s", dataSetUID, currentThreadName, archiveFileName));
            shellScriptResolver.runArchiveExtractedDataSet(dataSetUID, archiveFileName, ftpLocation, homePath, "workflow");
            logger.info(String.format("(dataSetUID=%d / threadName=%s) - Finish archiving the extracted data set: %s, Elapsed time: %d ms", dataSetUID, currentThreadName, archiveFileName, (System.currentTimeMillis() - archiveFileBeginTime)));

            final String ftpURI = String.format("%s/%s", ftpLocation, archiveFileName);
            dataIntegrationPlatformAPICaller.callCreateFtpInfo(requestInfo.getDataSetUID(), requestInfo.getUserID(), ftpURI);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
}