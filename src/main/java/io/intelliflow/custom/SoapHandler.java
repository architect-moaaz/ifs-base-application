package io.intelliflow.custom;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.intelliflow.service.KeyVaultService;
import net.minidev.json.JSONObject;
import org.jbpm.workflow.core.impl.DataDefinition;
import org.jbpm.workflow.core.impl.IOSpecification;
import org.jbpm.workflow.core.node.WorkItemNode;
import org.kie.kogito.internal.process.runtime.KogitoWorkItem;
import org.kie.kogito.internal.process.runtime.KogitoWorkItemHandler;
import org.kie.kogito.internal.process.runtime.KogitoWorkItemManager;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class SoapHandler implements KogitoWorkItemHandler {
    private final KeyVaultService keyVaultService;

    public SoapHandler(KeyVaultService keyVaultService) {
        this.keyVaultService = keyVaultService;
    }

    @Override
    public void executeWorkItem(KogitoWorkItem workItem, KogitoWorkItemManager manager) {
        Map<String, Object> results = null;
        HttpURLConnection connection = null;
        String soapResponse = null;
        int responseCode = 0;
        try {
            // Extract required parameters
            String urlStr = (String) workItem.getParameter("Url");
            String contentType = (String) workItem.getParameter("Content");
            String serviceName = (String) workItem.getParameter("Servicename");
            String serviceMethod = (String) workItem.getParameter("Methodname");
            String nameSpace = (String) workItem.getParameter("Namespace");
            Object dataModelObject = workItem.getParameter("Payload");
            String workSpaceName =  (String) workItem.getParameter("workspace");
            String appName =  (String) workItem.getParameter("appname");
            String dataKey =  (String) workItem.getParameter("datakey");
            String authenticationType =  (String) workItem.getParameter("authentication");


            // Open a connection to the SOAP API
            URL url = new URL(urlStr);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", contentType);
            connection.setDoOutput(true);
            if(authenticationType != null || !authenticationType.isBlank()){
                ObjectMapper objectMapper = new ObjectMapper();
                String keyVaultResponse  = keyVaultService.getData(workSpaceName,appName,dataKey);
                Map<String, String> keyVaultResponseMap = objectMapper.readValue(keyVaultResponse, new TypeReference<>() {
                });
                Map<String, String> dataJson = objectMapper.readValue(keyVaultResponseMap.get("data"), new TypeReference<>() {
                });

                if(!authenticationType.isEmpty() && authenticationType == "BasicAuthentication"){
                    String userName = dataJson.get("dbUserName");
                    String password = dataJson.get("dbPassword");

                    // Set Basic Authentication headers
                    String credentials = userName + ":" + password;
                    String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes());
                    connection.setRequestProperty("Authorization", "Basic " + encodedCredentials);
                }else if(!authenticationType.isEmpty() && authenticationType == "OAuth"){
                    String accessToken = (String) dataJson.get("accessToken");
                    connection.setRequestProperty("Authorization", "Bearer " + accessToken);
                }
            }

            // Write the SOAP request body to the connection's output stream
            OutputStream outputStream = connection.getOutputStream();
            String body = createBody(serviceName, serviceMethod, nameSpace, convertObjectIntoJsonString(dataModelObject));
            outputStream.write(body.getBytes());
            outputStream.flush();
            outputStream.close();

            // Send the SOAP request and read the response
            responseCode = connection.getResponseCode();
            BufferedReader reader;
            results = new HashMap<>();
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            // Process the SOAP response
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            // Do something with the SOAP response
            soapResponse = response.toString();
            // Process the SOAP response as needed
            System.out.println("response  = " + soapResponse);
            System.out.println("Status Code: " + responseCode);
            System.out.println("Response Body: " + soapResponse);
            if (responseCode == HttpURLConnection.HTTP_OK) {

                WorkItemNode node = (WorkItemNode) workItem.getNodeInstance().getNode();
                IOSpecification ioSpec = node.getIoSpecification();

                DataDefinition dataDefinition = ioSpec.getDataOutputs().get(0);

                // Create ObjectMapper instances for XML and JSON
                XmlMapper xmlMapper = new XmlMapper();
                ObjectMapper jsonMapper = new ObjectMapper();
//
//            // Convert XML to JSON
                JsonNode jsonNode1 = xmlMapper.readTree(soapResponse);
                String json = jsonMapper.writeValueAsString(jsonNode1);

                // Print the JSON output
                System.out.println("new response  = " + json);

                for (DataDefinition dDef : ioSpec.getDataOutputs()) {
                    if ("Result".equals(dDef.getLabel())) {
                        Class<?> clazz = Class.forName(dataDefinition.getType());
                        Object instance = clazz.newInstance();
                        Field[] fields = clazz.getDeclaredFields();
                        ObjectMapper objectMapper = new ObjectMapper();
                        JsonNode jsonNode = objectMapper.readTree(json);
                        for (Field field : fields) {
                            if (jsonNode.findValue(field.getName()) != null) {
                                JsonNode metaDataNode = jsonNode.findValue(field.getName());
                                String metaDataValue = metaDataNode.asText();
                                field.setAccessible(true);
                                field.set(instance, metaDataValue);
                            }
                        }
                        results.put("Result", instance);
                    } else if ("StatusCode".equals(dDef.getLabel())) {
                        results.put("StatusCode", String.valueOf(responseCode));
                    }
                }
                System.out.println(dataDefinition.getType());
            } else {
                results.put("errorCode", responseCode);
                results.put("error message ", soapResponse);
            }

            manager.completeWorkItem(workItem.getStringId(), results);
        } catch (Exception e) {
            // Handle any exceptions that may occur
            e.printStackTrace();
            // Optionally, you can complete the work item with an error if needed
            manager.abortWorkItem(workItem.getStringId());
        }
    }

    private String convertObjectIntoJsonString(Object dataModelObject) {
        JSONObject jsonObject = new JSONObject();
        try {
            Class<?> clazz = Class.forName(dataModelObject.getClass().getName());
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(dataModelObject);
                jsonObject.put(field.getName(), value);
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return jsonObject.toJSONString();
    }

    @Override
    public void abortWorkItem(KogitoWorkItem workItem, KogitoWorkItemManager manager) {
        // Handle work item abortion if necessary
    }

    private String createBody(String serviceName, String methodName, String nameSpace, String data ) {
        String xmlString = "";
//        String data = "{\"id\": \"123\", \"productName\": \"Example Product\", \"price\": \"9.99\", \"available\": true, \"ingredients\": {\"ingredient\": [\"Ingredient 1\", \"Ingredient 2\"]}, \"gst\": \"0.18\"}";
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            Document doc = docFactory.newDocumentBuilder().newDocument();

            // Create Envelope element
            Element envelope = doc.createElement("soapenv:Envelope");
            envelope.setAttribute("xmlns:soapenv", "http://schemas.xmlsoap.org/soap/envelope/");
            doc.appendChild(envelope);

            // Create Header element
            Element header = doc.createElement("soapenv:Header");
            envelope.appendChild(header);

            // Create Body element
            Element body = doc.createElement("soapenv:Body");
            envelope.appendChild(body);

            // Create addProduct element
            Element addProduct = doc.createElement(serviceName + ":" + methodName);
            addProduct.setAttribute("xmlns:" + serviceName, nameSpace);
            body.appendChild(addProduct);

            // Convert JSON data to XML elements
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(data);
            convertJsonToXmlElements(doc, addProduct, jsonNode);

            // Transform document to XML string
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);

            // Hold the XML in a String variable
            StreamResult result = new StreamResult(new StringWriter());
            transformer.transform(source, result);
            xmlString = result.getWriter().toString();

            System.out.println(xmlString); // Just to verify the generated XML string

        } catch (ParserConfigurationException | TransformerException | IOException e) {
            e.printStackTrace();
        }
        return xmlString;
    }

    private static void convertJsonToXmlElements(Document doc, Element parentElement, JsonNode jsonNode) {
        ObjectMapper objectMapper = new ObjectMapper();
        if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            objectNode.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();

                if (value.isObject() || value.isArray()) {
                    Element element = doc.createElement(key);
                    parentElement.appendChild(element);
                    convertJsonToXmlElements(doc, element, value);
                } else {
                    Element element = doc.createElement(key);
                    element.appendChild(doc.createTextNode(value.asText()));
                    parentElement.appendChild(element);
                }
            });
        } else if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (JsonNode node : arrayNode) {
                if (node.isObject()) {
                    convertJsonToXmlElements(doc, parentElement, node);
                } else {
                    Element element = doc.createElement(parentElement.getNodeName());
                    element.appendChild(doc.createTextNode(node.asText()));
                    parentElement.getParentNode().appendChild(element);
                }
            }
        }
    }
}
