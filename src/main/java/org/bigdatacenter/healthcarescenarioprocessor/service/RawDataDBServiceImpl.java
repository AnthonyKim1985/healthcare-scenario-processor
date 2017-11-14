package org.bigdatacenter.healthcarescenarioprocessor.service;

import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarescenarioprocessor.domain.extraction.TableCreationTask;
import org.bigdatacenter.healthcarescenarioprocessor.persistence.RawDataDBMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RawDataDBServiceImpl implements RawDataDBService {
    private final RawDataDBMapper rawDataDBMapper;

    @Autowired
    public RawDataDBServiceImpl(RawDataDBMapper rawDataDBMapper) {
        this.rawDataDBMapper = rawDataDBMapper;
    }

    @Override
    public void extractData(DataExtractionTask dataExtractionTask) {
        this.rawDataDBMapper.extractData(dataExtractionTask);
    }

    @Override
    public void createTable(TableCreationTask tableCreationTask) {
        this.rawDataDBMapper.createTable(tableCreationTask);
    }
}