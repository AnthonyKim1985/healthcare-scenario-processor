package org.bigdatacenter.healthcarescenarioprocessor.domain.workflow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioQuery implements Serializable {
    private String query;
    private String type;
}