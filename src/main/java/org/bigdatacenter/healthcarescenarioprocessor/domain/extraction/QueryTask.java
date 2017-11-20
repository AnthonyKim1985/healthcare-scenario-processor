package org.bigdatacenter.healthcarescenarioprocessor.domain.extraction;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class QueryTask implements Serializable {
    private TableCreationTask tableCreationTask;
    private DataExtractionTask dataExtractionTask;
}