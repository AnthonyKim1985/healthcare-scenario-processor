package org.bigdatacenter.healthcarescenarioprocessor.domain.workflow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bigdatacenter.healthcarescenarioprocessor.domain.transaction.TrRequestInfo;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkFlowRequest implements Serializable {
    private TrRequestInfo requestInfo;
    private ScenarioTask scenarioTask;
}
