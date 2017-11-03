package org.bigdatacenter.healthcarescenarioprocessor.api.caller.statistic;

public interface StatisticAPICaller {
    void callCreateStatistic(Integer dataSetUID, String databaseName, String tableName);
}