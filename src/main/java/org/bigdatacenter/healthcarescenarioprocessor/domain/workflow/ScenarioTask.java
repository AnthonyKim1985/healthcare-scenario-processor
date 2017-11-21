package org.bigdatacenter.healthcarescenarioprocessor.domain.workflow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioTask implements Serializable {
    private Integer dataSetUID;
    private List<ScenarioQuery> scenarioQueryList;
}
