package org.bigdatacenter.healthcarescenarioprocessor.domain.extraction;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class TableCreationTask implements Serializable {
    private String dbAndHashedTableName;
    private String query;
}
