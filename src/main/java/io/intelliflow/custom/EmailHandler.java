package io.intelliflow.custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.*;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kie.kogito.internal.process.runtime.KogitoWorkItem;
import org.kie.kogito.internal.process.runtime.KogitoWorkItemHandler;
import org.kie.kogito.internal.process.runtime.KogitoWorkItemManager;

import java.io.IOException;
import java.time.LocalDate;
import java.io.InputStream;
import java.util.*;

public class EmailHandler implements KogitoWorkItemHandler {

    private KafkaProducer<String, String> producer;
	
    private String workspace;
	private String appName;

    public static final String PARAM_TOUSER = "To";
    public static final String PARAM_CCUSER = "CC";
    public static final String PARAM_BCCUSER = "BCC";
    public static final String PARAM_SUBJECT = "Subject";
    public static final String PARAM_CONTENT = "Content";
    public static final String PARAM_CONTENTTYPE = "ContentType";

    public static final String PARAM_TEMPLATE = "Template Name";

    public static final String PARAM_MODEL = "Variable Model";

    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("application.properties");

    public EmailHandler() {
        Properties props = new Properties();
        Properties appProps = new Properties();
        try {
            appProps.load(inputStream);
            props.put("bootstrap.servers", appProps.get("kafka.bootstrap.servers").toString());
			workspace = appProps.get("ifs.app.workspace").toString();
            appName = appProps.get("ifs.app.miniappname").toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Bootstrap initiated::: " + props.get("bootstrap.servers").toString());
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    @Override
    public void executeWorkItem(KogitoWorkItem kogitoWorkItem, KogitoWorkItemManager kogitoWorkItemManager) {

        Map<String, Object> results = new HashMap<>();

        String toEmailAddress = (String) kogitoWorkItem.getParameter(PARAM_TOUSER);
        String ccEmailAddress = (String) kogitoWorkItem.getParameter(PARAM_CCUSER);
        String bccEmailAddress = (String) kogitoWorkItem.getParameter(PARAM_BCCUSER);
        String emailSubject = (String) kogitoWorkItem.getParameter(PARAM_SUBJECT);
        String emailContentType = (String) kogitoWorkItem.getParameter(PARAM_CONTENTTYPE);
        String templateName = (String) kogitoWorkItem.getParameter(PARAM_TEMPLATE);
        // TODO:Need more info on content
        String emailContent = (String) kogitoWorkItem.getParameter(PARAM_CONTENT);
		
		Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .registerTypeAdapter(LocalDate.class, new LocalDateAdapter())
                .create();
        JSONArray dataModelVariables = new JSONArray();
        if (Objects.nonNull(kogitoWorkItem.getParameter(PARAM_MODEL))) {
            JSONObject dataObj = new JSONObject();
            dataObj.put(kogitoWorkItem.getParameter(PARAM_MODEL).getClass().getSimpleName(),
                    gson.toJson(kogitoWorkItem.getParameter(PARAM_MODEL)));
            dataModelVariables.add(dataObj);
        }

        //If there are more variables present create a list
        for(String parameter : kogitoWorkItem.getParameters().keySet()){
            if(parameter.indexOf(PARAM_MODEL + "_") != -1) {
                JSONObject dataObj = new JSONObject();
                dataObj.put(kogitoWorkItem.getParameter(parameter).getClass().getSimpleName(),
                        gson.toJson(kogitoWorkItem.getParameter(parameter)));
                dataModelVariables.add(dataObj);
            }
        }

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> jsonMap = new HashMap<>();
        String finaJson = null;

        if (Objects.nonNull(toEmailAddress) &&
                Objects.nonNull(emailSubject)) {
            jsonMap.put("toAddress", stringToList(toEmailAddress));
            jsonMap.put("subject", emailSubject);
            if (Objects.nonNull(ccEmailAddress)) {
                jsonMap.put("ccAddress", stringToList(ccEmailAddress));
            }
            if (Objects.nonNull(bccEmailAddress)) {
                jsonMap.put("bccAddress", stringToList(bccEmailAddress));
            }
            if (Objects.nonNull(templateName)) {
                jsonMap.put("templateName", templateName);
            }
            jsonMap.put("processId", kogitoWorkItem.getProcessInstanceId());
            jsonMap.put("model", dataModelVariables);
			jsonMap.put("workspace", workspace);
			jsonMap.put("app", appName);

            try {
                finaJson = objectMapper.writeValueAsString(jsonMap);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

        }

        if (Objects.nonNull(finaJson)) {
            // send the event to the Kafka topic
            ProducerRecord<String, String> record = new ProducerRecord<>("email", finaJson);
            producer.send(record);
            results.put("Result", "Success");
        } else {
            // Email Sending Failed
            results.put("Result", "Failure");
        }
        kogitoWorkItemManager.completeWorkItem(kogitoWorkItem.getStringId(), results);
    }

    @Override
    public void abortWorkItem(KogitoWorkItem kogitoWorkItem, KogitoWorkItemManager kogitoWorkItemManager) {

    }

    private List<String> stringToList(String data) {
        List<String> dataList = new ArrayList<>();
        if (data.contains(";")) {
            // Creating a list of to address
            dataList = Arrays.asList(data.split(";"));
        } else {
            dataList.add(data);
        }
        return dataList;
    }
}
