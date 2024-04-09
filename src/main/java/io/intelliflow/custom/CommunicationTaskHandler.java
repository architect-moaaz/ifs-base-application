package io.intelliflow.custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import io.intelliflow.model.KafkaMessageWrapper;
import io.intelliflow.service.RouterService;
import io.intelliflow.utils.ObjectTransformer;
import io.quarkus.logging.Log;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;
import org.jbpm.workflow.core.impl.DataDefinition;
import org.jbpm.workflow.core.impl.IOSpecification;
import org.jbpm.workflow.core.node.WorkItemNode;
import org.kie.kogito.internal.process.runtime.KogitoWorkItem;
import org.kie.kogito.internal.process.runtime.KogitoWorkItemHandler;
import org.kie.kogito.internal.process.runtime.KogitoWorkItemManager;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.stream.StreamSupport;

public class CommunicationTaskHandler implements KogitoWorkItemHandler {

    private final KafkaProducer<String, String> producer;

    private String workspace;

    private String appName;

    private KafkaConsumer<String, String> consumer;

    private boolean currentlyConsuming = false;

    private final RouterService routerService;

    private static final String RESULT = "Result";

    private final ObjectTransformer objectTransformer;

    private final MongoClient mongodbClient;

    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("application.properties");

    public CommunicationTaskHandler(RouterService routerService, ObjectTransformer objectTransformer, MongoClient mongodbClient) {
        this.mongodbClient = mongodbClient;
        this.objectTransformer = objectTransformer;
        this.routerService = routerService;
        Properties props = new Properties();
        Properties appProps = new Properties();
        try {
            appProps.load(inputStream);
            props.put("bootstrap.servers", appProps.get("kafka.bootstrap.servers").toString());
            workspace = appProps.get("ifs.app.workspace").toString();
            appName = appProps.get("ifs.app.miniappname").toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Log.info("Bootstrap initiated::: " + props.get("bootstrap.servers").toString());
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    @Override
    public void executeWorkItem(KogitoWorkItem kogitoWorkItem, KogitoWorkItemManager kogitoWorkItemManager) {
        Log.info("Execute Custom Task");
        Map<String, Object> results = new HashMap<>();

        String user = getInitiatorUser(kogitoWorkItem.getProcessInstance().getStringId());
        Log.info("User Initiating: " + user);
        user = user == null ? "info@intelliflow.io" : user;

        //PUBLISH is the input assignment data model, which need to be sent across
        Object body = kogitoWorkItem.getParameter("PUBLISH");
        String allowList = (String) kogitoWorkItem.getParameter("allowList");
        String externalWorkspace = (String) kogitoWorkItem.getParameter("ExternalWorkspace");
        String externalApp = (String) kogitoWorkItem.getParameter("ExternalApp");
        String sendTopic = (String) kogitoWorkItem.getParameter("Acknowledge");
        String uniqueChannelName = (String) kogitoWorkItem.getParameter("UniqueChannelName");
        uniqueChannelName = uniqueChannelName == null ? "" : uniqueChannelName;
        String awaitTopic = (String) kogitoWorkItem.getParameter("Await");
        String processName = (String) kogitoWorkItem.getParameter("processName");
        String deviceSupport = (String) kogitoWorkItem.getParameter("device");
        String acknowledgeAnotherApp = (String) kogitoWorkItem.getParameter("AcknowledgeAnotherApp");
        deviceSupport = deviceSupport == null ? "B" : deviceSupport;
        String dataModelVariable = "datamodel";
        externalWorkspace = externalWorkspace == null ? workspace : externalWorkspace;
        externalApp = externalApp == null ? appName : externalApp;
        if (externalApp != null) {
            externalApp = externalApp.toLowerCase();
            externalApp = externalApp.replace(" ", "-");
        }
        if (deviceSupport.equalsIgnoreCase("desktop")) {
            deviceSupport = "D";
        } else if (deviceSupport.equalsIgnoreCase("mobile")) {
            deviceSupport = "M";
        }
        if (body != null) {
            Class<?> clazz1 = body.getClass();
            String[] className = clazz1.getName().split("\\.");
            dataModelVariable = className[className.length - 1];
            dataModelVariable = toCamelCase(dataModelVariable);
        }
        KafkaMessageWrapper kafkaMessageWrapper = new KafkaMessageWrapper();
        kafkaMessageWrapper.setData(body);
        kafkaMessageWrapper.setWorkspace(workspace);
        kafkaMessageWrapper.setAllowList(allowList);
        kafkaMessageWrapper.setSender(appName);
        if (processName != null) {
            startProcess(externalWorkspace, externalApp, processName, body, dataModelVariable, deviceSupport, user);
        }
        if (sendTopic != null && sendTopic.equalsIgnoreCase("true")) {
            ObjectMapper objectMapper = new ObjectMapper();
            String publishString;
            String acknowledgeTopic = (workspace + appName + kogitoWorkItem.getProcessInstance().getProcess().getName() + uniqueChannelName).toLowerCase();
            if(acknowledgeAnotherApp != null && acknowledgeAnotherApp.equalsIgnoreCase("true")){
                acknowledgeTopic = uniqueChannelName.toLowerCase();
            }
            acknowledgeTopic = acknowledgeTopic.replace("-", "");
            try {
                publishString = objectMapper.writeValueAsString(kafkaMessageWrapper);
                Log.info("publish string and topic : " + publishString + " & " + acknowledgeTopic);
                // send the event to the Kafka topic
                ProducerRecord<String, String> sentRecord = new ProducerRecord<>(acknowledgeTopic, publishString);
                producer.send(sentRecord);
            } catch (JsonProcessingException e) {
                results.put(RESULT, "Failed");
                e.printStackTrace();
            }
        }
        if (awaitTopic == null || awaitTopic.equalsIgnoreCase("false")) {
            results.put(RESULT, body);
            kogitoWorkItemManager.completeWorkItem(kogitoWorkItem.getStringId(), results);
        } else {
            WorkItemNode node = (WorkItemNode) kogitoWorkItem.getNodeInstance().getNode();
            IOSpecification ioSpec = node.getIoSpecification();
            DataDefinition dataDefinition = ioSpec.getDataOutputs().get(0);
            Class<?> clazz = null;
            for (DataDefinition dDef : ioSpec.getDataOutputs()) {
                if (RESULT.equals(dDef.getLabel())) {
                    try {
                        clazz = Class.forName(dataDefinition.getType());
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (!currentlyConsuming) {
                Properties kafkaProps = getConsumerConfig();
                currentlyConsuming = true;
                if (consumer == null) {
                    consumer = new KafkaConsumer<>(kafkaProps);
                }
                String topic = (externalWorkspace + externalApp + processName + uniqueChannelName).toLowerCase();
                if(acknowledgeAnotherApp != null && acknowledgeAnotherApp.equalsIgnoreCase("true")) {
                    topic = uniqueChannelName.toLowerCase();
                }
                topic = topic.replace("-", "");
                if (!consumer.subscription().contains(topic)) {
                    if (!consumer.listTopics().containsKey(topic)) {
                        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                        AdminClient adminClient = AdminClient.create(kafkaProps);
                        adminClient.createTopics(Collections.singletonList(newTopic));
                        adminClient.close();
                    }
                    consumer.subscribe(Collections.singletonList(topic));
                }
                Log.info("Waiting for response here: "+topic);
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    if (!records.isEmpty()) {
                        ConsumerRecord<String, String> latestRecord = records.records(new TopicPartition(topic, 0)).get(records.count() - 1);
                        handleCompletionMessage(latestRecord.value(), kogitoWorkItemManager, results, kogitoWorkItem, clazz);
                        Log.info("process completed for the workitem: " + kogitoWorkItem.getStringId() + " stopping the subscribe and committing the offset. The current offset: " + latestRecord.offset());
                        consumer.commitAsync();
                        consumer.unsubscribe();
                        currentlyConsuming = false;
                        break;
                    }
                }

            } else {
                consumer = null;
                this.currentlyConsuming = false;
                kogitoWorkItemManager.completeWorkItem(kogitoWorkItem.getStringId(), null);
            }
        }
    }

    private Properties getConsumerConfig(){
        Properties props = new Properties();
        try {
            props.load(getClass().getClassLoader().getResourceAsStream("application.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String bootstrapServers = props.get("kafka.bootstrap.servers").toString();
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "iac-app-group");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        Log.info("kafka properties: "+kafkaProps);
        return kafkaProps;
    }

    private String getInitiatorUser(String processId) {
        try {
            var database = mongodbClient.getDatabase("k1");
            Document query = new Document("processId", processId);
            FindIterable<Document> documents = database.getCollection("processes").find(query);
            for (Document document : documents) {
                String initiatedBy = document.getString("initiatedBy");
                if (initiatedBy != null) {
                    return initiatedBy;
                }
            }
            return null;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public static String toCamelCase(String input) {
        StringBuilder result = new StringBuilder();
        String[] words = input.split("[\\s_]+");
        for (String word : words) {
            result.append(word.substring(0, 1).toLowerCase())
                    .append(word.substring(1));
        }
        return result.toString();
    }

    private void startProcess(String extWorkspace, String extAppName, String processName, Object source, String dataModelVariable, String deviceSupport, String user) {
        try {
            Map<String, Object> responseObj = objectTransformer.transform(source, dataModelVariable);
            ObjectMapper objectMapper = new ObjectMapper();
            String context = routerService.getContext(extWorkspace, extAppName, deviceSupport);
            JsonNode jsonNode = objectMapper.readTree(context);
            JsonNode pathsNode = jsonNode.get("data").get("_paths");
            Log.info("Paths: "+pathsNode);
            Optional<String> matchingPath = StreamSupport.stream(pathsNode.spliterator(), false)
                    .filter(node -> processName.equalsIgnoreCase(node.get("endpoint_label").asText()))
                    .map(node -> node.get("path").asText())
                    .findFirst();
            if (matchingPath.isPresent()) {
                String path = matchingPath.get();
                Log.info("App path: "+path);
                String res = routerService.startProcess(extWorkspace, extAppName, path, user,user, "process", responseObj);
                Log.info("process start response: "+res);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void handleCompletionMessage(String value, KogitoWorkItemManager kogitoWorkItemManager, Map<String, Object> results, KogitoWorkItem kogitoWorkItem, Class<?> clazz) {
        ObjectMapper objectMapper = new ObjectMapper();
        KafkaMessageWrapper<Object> kafkaMessageWrapper = null;
        try {
            kafkaMessageWrapper = objectMapper.readValue(value, new TypeReference<>() {
            });
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        if (kafkaMessageWrapper != null) {
            LinkedHashMap<?, ?> data = null;
            try {
                data = objectMapper.readValue(value, LinkedHashMap.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            if (data != null && !data.isEmpty() && data.containsKey("data") && data.get("data") != null) {
                Object convertedData = objectMapper.convertValue(data.get("data"), clazz);
                results.put(RESULT, convertedData);
                kogitoWorkItemManager.completeWorkItem(kogitoWorkItem.getStringId(), results);
                Log.info("completed workflow for processId: " + kogitoWorkItem.getStringId());
            } else {
                results.put(RESULT, null);
                kogitoWorkItemManager.completeWorkItem(kogitoWorkItem.getStringId(), results);
                Log.info("completed workflow for processId without data model: " + kogitoWorkItem.getStringId());
            }
        }
    }

    @Override
    public void abortWorkItem(KogitoWorkItem kogitoWorkItem, KogitoWorkItemManager kogitoWorkItemManager) {

    }
}