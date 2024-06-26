package es.uca;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.espertech.esper.runtime.client.DeploymentStateEventDeployed;
import com.espertech.esper.runtime.client.DeploymentStateEventUndeployed;
import com.espertech.esper.runtime.client.DeploymentStateListener;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esperio.kafka.EsperIOKafkaOutputFlowController;
import com.espertech.esperio.kafka.EsperIOKafkaOutputFlowControllerContext;

public class CustomFlowController implements EsperIOKafkaOutputFlowController {
    @SuppressWarnings("rawtypes")
    private KafkaProducer producer;
    private EPRuntime runtime;

    @Override
    public void initialize(EsperIOKafkaOutputFlowControllerContext context) {
        this.runtime = context.getRuntime();

        try {
            producer = new KafkaProducer<>(context.getProperties());
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        String[] deploymentIds = runtime.getDeploymentService().getDeployments();
        for (String depoymentId : deploymentIds) {
            for (EPStatement statement : context.getRuntime().getDeploymentService()
                    .getDeployment(depoymentId).getStatements()) {
                if (statement != null) {
                    statement.addListener(
                            new KafkaListener(producer, statement.getEventType().getName()));
                }
            }
        }

        runtime.getDeploymentService().addDeploymentStateListener(new DeploymentStateListener() {
            public void onDeployment(DeploymentStateEventDeployed event) {
                for (EPStatement statement : event.getStatements()) {
                    statement.addListener(
                            new KafkaListener(producer, statement.getEventType().getName()));
                }
            }

            public void onUndeployment(DeploymentStateEventUndeployed event) {
            }
        });
    }

    @Override
    public void close() {
        producer.close();
    }
}
