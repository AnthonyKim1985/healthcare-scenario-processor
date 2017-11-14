package org.bigdatacenter.healthcarescenarioprocessor.service;


import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.TableCreationTask;

public interface RawDataDBService {
    void extractData(DataExtractionTask dataExtractionTask);

    void createTable(TableCreationTask tableCreationTask);
}
