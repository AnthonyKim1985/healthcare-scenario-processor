package org.bigdatacenter.healthcarescenarioprocessor.api;

import org.bigdatacenter.healthcarescenarioprocessor.api.caller.DataIntegrationPlatformAPICaller;
import org.bigdatacenter.healthcarescenarioprocessor.config.RabbitMQConfig;
import org.bigdatacenter.healthcarescenarioprocessor.domain.workflow.ScenarioTask;
import org.bigdatacenter.healthcarescenarioprocessor.exception.RESTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
@RequestMapping("/workflow/api")
public class DataWorkFlowController {
    private static final Logger logger = LoggerFactory.getLogger(DataWorkFlowController.class);
    private static final String currentThreadName = Thread.currentThread().getName();
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final RabbitTemplate rabbitTemplate;
    private final DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller;

    @Autowired
    public DataWorkFlowController(RabbitTemplate rabbitTemplate, DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller) {
        this.rabbitTemplate = rabbitTemplate;
        this.dataIntegrationPlatformAPICaller = dataIntegrationPlatformAPICaller;
    }

    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(value = "dataWorkFlow", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public String dataWorkFlow(@RequestBody ScenarioTask scenarioTask, HttpServletResponse httpServletResponse) {
        try {
            synchronized (this) {
                rabbitTemplate.convertAndSend(RabbitMQConfig.EXTRACTION_REQUEST_QUEUE, scenarioTask);
            }
        } catch (Exception e) {
            throw new RESTException(String.format("(threadName=%s) - Bad request (%s)", currentThreadName, e.getMessage()), httpServletResponse);
        }

        return dateFormat.format(new Date(System.currentTimeMillis()));
    }
}
