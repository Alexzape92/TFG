package es.uca;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import com.espertech.esperio.kafka.EsperIOKafkaConfig;
import com.espertech.esperio.kafka.EsperIOKafkaInputAdapterPlugin;
import com.espertech.esperio.kafka.EsperIOKafkaInputSubscriberByTopicList;
import com.espertech.esperio.kafka.EsperIOKafkaOutputAdapterPlugin;
import com.espertech.esperio.kafka.KafkaOutputDefault;

public class App {
    private static String brokerIp;

    private static Configuration configuration;
    public static String[] simpleEventTypes;

    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            props.load(new FileInputStream("config/dcep.properties"));
            brokerIp = props.getProperty("brokerIp");
            simpleEventTypes = props.getProperty("simpleEventTypes").split(",");
            setConfiguration();

            String patterns = new String(Files.readAllBytes(Paths.get("config/patterns.epl")));
            compileAndDeploy(patterns);

            while (true) {
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    private static void setConfiguration() {
        configuration = new Configuration();

        configuration.getCommon().addImport(KafkaOutputDefault.class);
        configureKafka();
    }

    private static void configureKafka() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerIp);

        // Create input topics if it doesn't exist
        try {
            Properties adminProps = new Properties();
            adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerIp);
            Admin admin = Admin.create(adminProps);
            Set<String> topics = admin.listTopics().names().get();
            for (String topic : simpleEventTypes) {
                if (!topics.contains(topic)) {
                    List<NewTopic> newTopicList = new ArrayList<>();
                    NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                    newTopicList.add(newTopic);
                    admin.createTopics(newTopicList);
                }
            }
            admin.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        // Kafka Consumer Properties
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        // EsperIO Kafka Input Adapter Properties
        props.put(EsperIOKafkaConfig.INPUT_SUBSCRIBER_CONFIG, EsperIOKafkaInputSubscriberByTopicList.class.getName());
        props.put(EsperIOKafkaConfig.TOPICS_CONFIG, String.join(",", simpleEventTypes));
        props.put(EsperIOKafkaConfig.INPUT_PROCESSOR_CONFIG, CustomJsonProcessor.class.getName());

        // Kafka Producer Properties
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerIp);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // EsperIO Kafka Output Adapter Properties
        props.put(EsperIOKafkaConfig.OUTPUT_FLOWCONTROLLER_CONFIG,
                CustomFlowController.class.getName());

        configuration.getRuntime().addPluginLoader("KafkaInput", EsperIOKafkaInputAdapterPlugin.class.getName(), props,
                null);

        configuration.getRuntime().addPluginLoader("KafkaOutput", EsperIOKafkaOutputAdapterPlugin.class.getName(),
                props, null);
    }

    private static EPDeployment compileAndDeploy(String epl) throws EPCompileException, EPDeployException {
        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        CompilerArguments args = new CompilerArguments(configuration);

        EPCompiled epCompiled = compiler.compile(epl, args);
        EPDeployment deployment = runtime.getDeploymentService().deploy(epCompiled);

        return deployment;
    }
}
