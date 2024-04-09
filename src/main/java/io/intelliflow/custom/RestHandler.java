package io.intelliflow.custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.intelliflow.service.KeyVaultService;
import io.quarkus.logging.Log;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jbpm.workflow.core.impl.DataDefinition;
import org.jbpm.workflow.core.impl.IOSpecification;
import org.jbpm.workflow.core.node.WorkItemNode;
import org.kie.kogito.internal.process.runtime.KogitoWorkItem;
import org.kie.kogito.internal.process.runtime.KogitoWorkItemHandler;
import org.kie.kogito.internal.process.runtime.KogitoWorkItemManager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiConsumer;

public class RestHandler implements KogitoWorkItemHandler {

    private static final int DEFAULT_TOTAL_POOL_CONNECTIONS = 500;
    private static final int DEFAULT_MAX_POOL_CONNECTIONS_PER_ROUTE = 50;
    protected static final String USE_SYSTEM_PROPERTIES = "org.kie.workitem.rest.useSystemProperties";

    public static final String PARAM_AUTH_TYPE = "AuthType";
    public static final String PARAM_CONNECT_TIMEOUT = "ConnectTimeout";
    public static final String PARAM_READ_TIMEOUT = "ReadTimeout";
    public static final String PARAM_CONTENT_TYPE = "ContentType";
    public static final String PARAM_CONTENT_TYPE_CHARSET = "ContentTypeCharset";
    public static final String PARAM_HEADERS = "Headers";
    public static final String PARAM_CONTENT = "Content";
    public static final String PARAM_CONTENT_DATA = "ContentData";
    public static final String PARAM_USERNAME = "UserName";
    public static final String PARAM_PASSWORD = "Password";
    public static final String PARAM_AUTHURL = "AuthUrl";
    public static final String PARAM_RESULT = "Result";
    public static final String PARAM_STATUS = "Status";
    public static final String PARAM_STATUS_MSG = "StatusMsg";
    public static final String PARAM_COOKIE = "Cookie";
    public static final String PARAM_COOKIE_PATH = "CookiePath";

    private final KeyVaultService keyVaultService;

    public enum AuthenticationType {
        NONE, BASIC, FORM_BASED
    }

    private String workspace;

    final ObjectMapper objectMapper;
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("application.properties");

    public RestHandler(KeyVaultService keyVaultService) {
        this.keyVaultService = keyVaultService;
        objectMapper = new ObjectMapper();
        Properties appProps = new Properties();
        try {
            appProps.load(inputStream);
            workspace = appProps.get("ifs.app.workspace").toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void executeWorkItem(KogitoWorkItem workItem, KogitoWorkItemManager manager) {


        // extract required parameters
        String urlStr = (String) workItem.getParameter("Url");
        String method = (String) workItem.getParameter("Method");
        String contentType = (String) workItem.getParameter(PARAM_CONTENT_TYPE);

        String acceptHeader = (String) workItem.getParameter("AcceptHeader");
        String acceptCharset = (String) workItem.getParameter("AcceptCharset");
        String headers = (String) workItem.getParameter(PARAM_HEADERS);
        String cookie = (String) workItem.getParameter(PARAM_COOKIE);
        String cookiePath = (String) workItem.getParameter(PARAM_COOKIE_PATH);
        String dataKey = (String) workItem.getParameter("dataKey");
        String responseToken = (String) workItem.getParameter("responseToken");
        String responseType = (String) workItem.getParameter("responseType");
        responseToken = responseToken == null ? "access_token" : responseToken;
        String[] tokenAccess = responseToken.split(",");
        Object listOfHttpParts = workItem.getParameter("ListOfParts");
        Object body = workItem.getParameter("body");
        String requestType = (String) workItem.getParameter("RequestType");
        String xmlKeysCase = (String) workItem.getParameter("xmlKeysCase");
        String bodyStr = "";
        xmlKeysCase = xmlKeysCase == null ? "" : xmlKeysCase;
        requestType = requestType == null ? "" : requestType;
        responseType = responseType == null ? "" : responseType;

        if (body instanceof String) {
            bodyStr = (String) body;
            Log.info("Body is string");
        } else if (body != null) {
                Gson gson = new GsonBuilder().disableHtmlEscaping().create();
                bodyStr = gson.toJson(body);
                bodyStr = bodyStr.replaceAll("\u003d","=");
        }
        String dataModelVariable = "";
        if (body != null) {
            Class<?> clazz1 = body.getClass();
            String[] className = clazz1.getName().split("\\.");
            dataModelVariable = className[className.length - 1];
            dataModelVariable = toCamelCase(dataModelVariable);
        }
        if(requestType.equalsIgnoreCase("xml")){
            try {
                bodyStr = convertJsonToXml(bodyStr, dataModelVariable, xmlKeysCase);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }

        if (urlStr == null) {
            throw new IllegalArgumentException("Url is a required parameter");
        }

        Builder httpRequestBuilder;
        HttpPost httpPost = new HttpPost(urlStr);
        try {
            URL parsedURL = new URL(urlStr);
            String query = parsedURL.getQuery();
            if (query != null) {
                String[] queryParams = query.split("&");
                StringBuilder encodedQuery = new StringBuilder();
                for (String param : queryParams) {
                    String[] parts = param.split("=");
                    if (parts.length == 2) {
                        String paramName = parts[0];
                        String paramValue = parts[1];
                        String encodedParamValue = URLEncoder.encode(paramValue, StandardCharsets.UTF_8);
                        encodedQuery.append(paramName).append("=").append(encodedParamValue).append("&");
                    } else {
                        encodedQuery.append(param).append("&");
                    }
                }
                encodedQuery.setLength(encodedQuery.length() - 1);
                urlStr = parsedURL.getProtocol() + "://" + parsedURL.getAuthority() + parsedURL.getPath() + "?" + encodedQuery;
            }
            Log.info("Original URL: " + urlStr);

            if (method == null || method.trim().length() == 0) {
                method = "GET";
            }

            httpRequestBuilder = HttpRequest.newBuilder(new URI(urlStr));

            if ("GET".equals(method)) {
                httpRequestBuilder.GET();
            } else if ("POST".equals(method)) {

                if ((body != null) && !bodyStr.isEmpty()) {
                    httpRequestBuilder.POST(BodyPublishers.ofString(bodyStr));
                } else {
                    httpRequestBuilder.POST(BodyPublishers.noBody());
                }

            } else if ("PUT".equals(method)) {

                if ((body != null) && !bodyStr.isEmpty()) {
                    httpRequestBuilder.PUT(BodyPublishers.ofString(bodyStr));
                } else {
                    httpRequestBuilder.PUT(BodyPublishers.noBody());
                }

            } else if ("DELETE".equals(method)) {

                httpRequestBuilder.DELETE();

            }

            //check if authentication is required
            String authType = (String) workItem.getParameter(PARAM_AUTH_TYPE);
            if (AuthenticationType.BASIC.name().equals(authType)) {

                String userName = (String) workItem.getParameter(PARAM_USERNAME);
                String password = (String) workItem.getParameter(PARAM_PASSWORD);

                if (userName == null || userName.isBlank() || userName.isEmpty()) {
                    throw new IllegalArgumentException("For BASIC authentication UserName is required");
                }
                if (password == null || password.isBlank() || password.isEmpty()) {
                    throw new IllegalArgumentException("For BASIC authentication Password is required");
                }

                String authString = "";
                byte[] encodedAuth = Base64.getEncoder().encode(authString.getBytes(StandardCharsets.UTF_8));
                String authHeaderValue = "Basic " + new String(encodedAuth);
                httpRequestBuilder.headers("Authorization", authHeaderValue);

            } else if (AuthenticationType.FORM_BASED.equals(authType)) {
                //TODO: Need to implement the form based authentication over here
            }


            if (dataKey != null && !dataKey.isEmpty()) {
                String keyVaultResponse = keyVaultService.getData(workspace, "", dataKey);
                Map<String, String> keyVaultResponseMap = objectMapper.readValue(keyVaultResponse, new TypeReference<>() {
                });
                Map<String, String> jsonMap = objectMapper.readValue(keyVaultResponseMap.get("data"), new TypeReference<>() {
                });
                String authHeader = removeQuotesFromJson(jsonMap.get("Headers"));
                String authMethod = removeQuotesFromJson(jsonMap.get("Methods"));
                String authUrl = removeQuotesFromJson(jsonMap.get("Url"));
                String authBody = jsonMap.get("Body");

                Builder oAuth = HttpRequest.newBuilder(new URI(authUrl));
                addCustomHeaders(authHeader, oAuth::setHeader);
                if (authMethod.equalsIgnoreCase("post")) {
                    if (!authBody.isEmpty()) {
                        oAuth.POST(BodyPublishers.ofString(authBody));
                    } else {
                        oAuth.POST(BodyPublishers.noBody());
                    }
                } else if (authMethod.equalsIgnoreCase("get")) {
                    oAuth.GET();
                } else if (authMethod.equalsIgnoreCase("put")) {
                    if (!authBody.isEmpty()) {
                        oAuth.PUT(BodyPublishers.ofString(authBody));
                    } else {
                        oAuth.PUT(BodyPublishers.noBody());
                    }
                }

                HttpRequest httpRequestoAuth = oAuth.build();
                HttpClient clientoAuth = HttpClient.newHttpClient();
                clientoAuth.send(httpRequestoAuth, HttpResponse.BodyHandlers.ofString());
                HttpResponse<String> response = clientoAuth.send(httpRequestoAuth, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    JsonNode jsonNode = objectMapper.readTree(response.body());
                    String token = getAccessTokenFromNestedJson(jsonNode, tokenAccess);
                    Log.info(" Authorization token: " + token);
                    if (token != null && !token.startsWith("Bearer ")) {
                        token = "Bearer " + token;
                    }
                    httpPost.setHeader("Authorization", token);
                    httpRequestBuilder.header("Authorization", token);
                }
            }
            String responseBody;
            int statusCode;
            if (listOfHttpParts != null) {
                Log.info("Images in request body");
                List<Map<String, Object>> fileUrls = objectMapper.convertValue(listOfHttpParts, new TypeReference<>() {
                });
                HttpEntity httpEntity = createMultipartEntity(fileUrls);
                httpPost.setEntity(httpEntity);
                try (CloseableHttpClient httpClient = HttpClients.createDefault();
                     CloseableHttpResponse closeableHttpResponse = httpClient.execute(httpPost)) {
                    responseBody = EntityUtils.toString(closeableHttpResponse.getEntity());
                    statusCode = closeableHttpResponse.getStatusLine().getStatusCode();
                }
            } else {
                Log.info("No images in request body.");
                //Set the headers given by the user
                if (headers != null && !headers.isBlank() && !headers.isEmpty()) {
                    addCustomHeaders(headers, httpRequestBuilder::setHeader);
                }
                //Execute and handle the Response
                HttpRequest httpRequest = httpRequestBuilder.build();
                // Create a HTTP client
                HttpClient client = HttpClient.newHttpClient();
                // Send the request and receive the response
                HttpResponse<String> response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                // Print the status code and response body
                responseBody = response.body();
                statusCode = response.statusCode();
            }
            if(responseType.equalsIgnoreCase("xml")){
                responseBody = convertXmlToJson(responseBody);
            }
            Log.info("Response Body: " + responseBody+" Response Code: "+statusCode);
            Map<String, Object> results = new HashMap<>();
            WorkItemNode node = (WorkItemNode) workItem.getNodeInstance().getNode();
            IOSpecification ioSpec = node.getIoSpecification();
            DataDefinition dataDefinition = ioSpec.getDataOutputs().get(0);
            for (DataDefinition dDef : ioSpec.getDataOutputs()) {
                if ("Response".equals(dDef.getLabel())) {
                    Class<?> clazz = Class.forName(dataDefinition.getType());
                    results.put("Response", new Gson().fromJson(responseBody, clazz));
                } else if ("StatusCode".equals(dDef.getLabel())) {
                    results.put("StatusCode", String.valueOf(statusCode));
                }
            }
            Log.info(dataDefinition.getType());
            manager.completeWorkItem(workItem.getStringId(), results);
        } catch (Exception e ) {
            manager.completeWorkItem(workItem.getStringId(), null);
            e.printStackTrace();
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

    public String convertJsonToXml(String json, String parentClass,String xmlKeysCase) throws JsonProcessingException {
        if(xmlKeysCase.equalsIgnoreCase("upper")){
            parentClass = parentClass.toUpperCase();
        }else if(xmlKeysCase.equalsIgnoreCase("lower")){
            parentClass = parentClass.toLowerCase();
        }
        JsonNode rootNode = objectMapper.readTree(json);
        JsonNode rootNodeUpperCase = convertKeysToUppercase(rootNode, xmlKeysCase);
        XmlMapper xmlMapper = new XmlMapper();
        xmlMapper.enable(SerializationFeature.INDENT_OUTPUT);
        xmlMapper.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, true);
        return xmlMapper.writeValueAsString(rootNodeUpperCase).replace("ObjectNode", parentClass);
    }
    public static JsonNode convertKeysToUppercase(JsonNode node, String xmlKeysCase) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            ObjectNode newNode = objectNode.objectNode();
            Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                if(!entry.getKey().equalsIgnoreCase("_id")) {
                    String newKey = entry.getKey();
                    if(xmlKeysCase.equalsIgnoreCase("upper")){
                        newKey = newKey.toUpperCase();
                    }else if(xmlKeysCase.equalsIgnoreCase("lower")){
                        newKey = newKey.toLowerCase();
                    }
                    JsonNode value = entry.getValue();
                    newNode.set(newKey, convertKeysToUppercase(value, xmlKeysCase));
                }
            }
            return newNode;
        } else if (node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;
            ArrayNode newArray = arrayNode.arrayNode();
            for (JsonNode value : arrayNode) {
                newArray.add(convertKeysToUppercase(value,xmlKeysCase));
            }
            return newArray;
        } else {
            return node;
        }
    }
    private static String getAccessTokenFromNestedJson(JsonNode node, String[] keys) {
        JsonNode currentNode = node;
        for (String key : keys) {
            currentNode = currentNode.get(key);
            if (currentNode == null) {
                return null;
            }
        }
        if (currentNode.isTextual()) {
            return currentNode.asText();
        }
        return null;
    }

    public static String removeQuotesFromJson(String jsonString) {
        return jsonString.replace("\"", "");
    }

    @Override
    public void abortWorkItem(KogitoWorkItem workItem, KogitoWorkItemManager manager) {

    }


    public void addCustomHeaders(String headers, BiConsumer<String, String> consumer) {

        for (String h : headers.split(";")) {
            String[] headerParts = h.split("=", 2);
            if (headerParts.length == 2) {
                consumer.accept(headerParts[0], headerParts[1]);
            }
        }
    }

    private static Path downloadFileFromUrl(String imageUrl, String imageName) throws IOException {
        Path tempDir = Files.createTempDirectory("image_downloads");
        String fileName = imageName + ".jpg";
        Path tempFilePath = tempDir.resolve(fileName);
        try (InputStream is = new URL(imageUrl).openStream();
             OutputStream os = Files.newOutputStream(tempFilePath)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            return tempFilePath;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String convertXmlToJson(String xmlString) throws JsonProcessingException {
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode jsonNode = xmlMapper.readTree(xmlString);
        jsonNode = convertKeysToUppercase(jsonNode,"camel");
        return objectMapper.writeValueAsString(jsonNode);
    }

    public static HttpEntity createMultipartEntity(List<Map<String, Object>> imageUrls) throws IOException {
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        Log.info("Image Urls: " + imageUrls);

        Map<String, ContentType> contentTypeMap = Map.of(
                "image/jpeg", ContentType.IMAGE_JPEG,
                "image/png", ContentType.IMAGE_PNG,
                "image/gif", ContentType.IMAGE_GIF,
                "application/json", ContentType.APPLICATION_JSON,
                "text/plain", ContentType.TEXT_PLAIN,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ContentType.APPLICATION_OCTET_STREAM
                // Add more content types as necessary
        );

        for (Map<String, Object> imageUrlMap : imageUrls) {
            String name = (String) imageUrlMap.get("name");
            String contentType = (String) imageUrlMap.get("contentType");
            String fileName = (String) imageUrlMap.get("fileName");
            String content = (String) imageUrlMap.get("content");
            ContentType partContentType = contentTypeMap.get(contentType);

            if (partContentType != null) {
                if (partContentType == ContentType.APPLICATION_JSON || partContentType == ContentType.TEXT_PLAIN) {
                    builder.addTextBody(name, content, partContentType);
                } else {
                    Path fileFilePath = downloadFileFromUrl(content, fileName);
                    File file = new File(fileFilePath.toUri());
                    FileBody fileBody = new FileBody(file, partContentType, fileName);
                    builder.addPart(name, fileBody);
                }
            } else {
                // Handle unsupported content types here
                Log.error("Unsupported content type: " + contentType);
            }
        }
        return builder.build();
    }
}
