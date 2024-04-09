package io.intelliflow.lifecycle;

import com.jayway.jsonpath.JsonPath;
import io.intelliflow.utils.CustomException;
import io.intelliflow.utils.Status;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.commons.io.IOUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

@Singleton
public class BaseAppLifecycle {

    @Inject
    @ConfigProperty(name = "ifs.app.workspace")
    String workspace;

    @Inject
    @ConfigProperty(name = "ifs.app.miniappname")
    String miniApp;

    @Inject
    @ConfigProperty(name = "ifs.server.url")
    String serverUrl;

    public static String workFlowJson;

    // Client instance to pass schema
    private final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .build();

    /*
     * To fetch the workflow descriptor file and load the value into the
     * static variable. It will later be used to fetch form content
     */
    void onStart(@Observes StartupEvent ev) throws IOException, CustomException {
        String fileName = (workspace + "-" + miniApp).toLowerCase() + ".wid";
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("workflow/" + fileName)) {
            workFlowJson = IOUtils.toString(in, StandardCharsets.UTF_8);
            Log.info("Workflow descriptor loaded");

            HttpRequest postRequest = HttpRequest.newBuilder()
                    .POST(HttpRequest.BodyPublishers.ofString(String.valueOf(getSchemaStructure())))
                    .setHeader("Content-Type", "application/json")
                    .uri(URI.create("http://" + serverUrl + "/register/" + workspace + "/" + miniApp))
                    .build();

            HttpResponse<String> response = httpClient.send(postRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() <= 300) {
                Log.info("Schema Registration was success!!");
            } else {
                Log.error("Schema Registration Failed!!");
                // throw new CustomException("Schema Registration Failed!!","500");
            }

        } catch (NullPointerException e) {
            e.printStackTrace();
            Log.error("No workflow Descriptor File Available for App. Discouraging the use of form content API");
            // throw new CustomException("No workflow Descriptor File Available for App. Discouraging the use of form content API", "500");
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            Log.error("Schema API Invocation Failed. Database not initialized.");
            // throw new CustomException("Schema API Invocation Failed. Database not initialized.","500");
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        System.out.println("stopped");
    }

    /*
        Getting the Schema structure generated form the
        wid file and meta files for data models
     */
    public JSONObject getSchemaStructure() throws CustomException {
        JSONObject schemaObj = new JSONObject();

        schemaObj.put("workspacename", workspace);
        schemaObj.put("appname", miniApp);

        if(Objects.nonNull(BaseAppLifecycle.workFlowJson)) {
            //Getting from BPMN Meta
            List<String> bpmnMetaFiles = getFiles("bpmn", true);
            JSONArray mappingArray = new JSONArray();
            for(String fileName : bpmnMetaFiles) {

                try {
                    String metaContent = IOUtils.toString(
                            getClass().getClassLoader().getResourceAsStream("meta/bpmn/" + fileName),
                            StandardCharsets.UTF_8
                    );
                    JSONArray metaArray = JsonPath.parse(metaContent).read("$.*");
                    for (int i = 0; i < metaArray.size(); i++) {
                        JSONObject mapObj = new JSONObject();
                        String name = (String) ((LinkedHashMap) metaArray.get(i)).get("name");
                        String nameSpace = (String) ((LinkedHashMap) metaArray.get(i)).get("namespace");
                        //Only DataModels Considered, Primitives are skipped
                        if(nameSpace.indexOf(".") != -1){
                            mapObj.put("modelName", nameSpace.substring(nameSpace.lastIndexOf(".") + 1));
                            mapObj.put("variableName", name);
                            mapObj.put("nameSpace", nameSpace.substring(0,nameSpace.lastIndexOf(".")));
                            mappingArray.add(mapObj);
                        }
                        System.out.println("Loaded "+ fileName + " BPMN Relation");
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                    // throw new CustomException("some IO file issue","500");
                }

            }
            schemaObj.put("mappings", mappingArray);

            //Creating models Array from DataModel meta
            List<String> metaFiles = getFiles("datamodel", true);
            JSONObject modelObj = new JSONObject();
            JSONObject entitySets = new JSONObject();
            JSONObject entityObj = new JSONObject();
            List<String> complexTypeList = new ArrayList<>();
            for(String fileName : metaFiles) {
                String modelName = fileName.substring(0, fileName.lastIndexOf("."));
                JSONArray maps = JsonPath.parse(mappingArray.toString()).read("$.[?(@.modelName=='" + modelName + "')]");
                try{
                    String metaContent = IOUtils.toString(
                            getClass().getClassLoader().getResourceAsStream("meta/datamodel/" + fileName),
                            StandardCharsets.UTF_8
                    );
                    JSONArray metaArray = JsonPath.parse(metaContent).read("$.*");
                    JSONObject entityType = new JSONObject();
                    entityType.put("entityType", "io.intelliflow.generated.models."+modelName);
                    entitySets.put(
                            //Character.toLowerCase(modelName.charAt(0)) + modelName.substring(1),
                            Character.toUpperCase(modelName.charAt(0)) + modelName.substring(1),
                            entityType);

                    if(maps.size() > 0){
                        modelObj.put("namespace", (String) ((LinkedHashMap) maps.get(0)).get("nameSpace"));
                    } else {
                        modelObj.put("namespace", "io.intelliflow.generated.models");
                    }
                    JSONObject nameObj = new JSONObject();
                    for(int i =0; i < metaArray.size(); i++) {
                        String name = (String) ((LinkedHashMap) metaArray.get(i)).get("name");
                        JSONObject typeObj = new JSONObject();
                        if((boolean) ((LinkedHashMap) metaArray.get(i)).get("isPrimitive")) {
                            String type;
                            if((boolean) ((LinkedHashMap) metaArray.get(i)).get("isCollection")) {
                                type = "Collection(" + getEdmType((String) ((LinkedHashMap) metaArray.get(i)).get("type")) + ")";
                            } else {
                                type = getEdmType((String) ((LinkedHashMap) metaArray.get(i)).get("type"));
                            }
                            typeObj.put("type", type);
                        } else {
                            if(!((boolean) ((LinkedHashMap) metaArray.get(i)).get("isCollection"))){
                                String type = "io.intelliflow.generated.models." + ((LinkedHashMap) metaArray.get(i)).get("type");
                                String relationship = type+"_"+modelName;
                                typeObj.put("type", type);
                                typeObj.put("relationship",relationship);
                            }else {
                                String type = "Collection(io.intelliflow.generated.models." + ((String) ((LinkedHashMap) metaArray.get(i)).get("type")) + ")";
                                typeObj.put("type", type);

                                //Create a lis of Complex types required
                                complexTypeList.add(((String) ((LinkedHashMap) metaArray.get(i)).get("type")));
                            }
                        }
                        nameObj.put(name, typeObj);

                    }
                    //Hadrcoded key id for now
                    JSONObject keyObj = new JSONObject();
                    keyObj.put("type", "Edm.String");
                    keyObj.put("key", true);
                    nameObj.put("_id", keyObj);
                    entityObj.put(modelName, nameObj);
                } catch (IOException e) {
                    e.printStackTrace();
                    // throw new CustomException("some IO file issue","500");
                }
            }
            if(complexTypeList.size() > 0) {
                Log.info("Complex Types are found and added to Schema");
                modelObj.put("complexTypes", generateComplexTypes(complexTypeList));
            }
            modelObj.put("entityTypes", entityObj);
            modelObj.put("entitySets", entitySets);
            schemaObj.put("models", modelObj);
        }
        return schemaObj;
    }


    public List<String> getFiles(String fileType, boolean ifMeta) throws CustomException {
        List<String> files = new ArrayList<String>();
        String filePath = null;
        if(ifMeta) {
            filePath = "meta/" + fileType;
        } else {
            filePath = fileType;
        }
        try {
            URI uri = getClass().getClassLoader().getResource(filePath).toURI();
            Path myPath;
            if (uri.getScheme().equals("jar")) {
                FileSystem fileSystem ;
                try {
                    fileSystem = FileSystems.newFileSystem(uri, Collections.<String, Object>emptyMap());
                } catch (FileSystemAlreadyExistsException e) {
                    fileSystem = FileSystems.getFileSystem(uri);
                }
                myPath = fileSystem.getPath(filePath);
            } else {
                myPath = Paths.get(uri);
            }
            Stream<Path> walk = Files.walk(myPath, 1);
            for (Iterator<Path> it = walk.iterator(); it.hasNext();){
                String fileName = it.next().getFileName().toString();
                if(fileName.indexOf("." + fileType) != -1){
                    files.add(fileName);
                }
                if(ifMeta && fileName.indexOf(".meta") != -1){
                    files.add(fileName);
                }
            }
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            // throw new CustomException("a string could not be parsed as a URI reference","500");
        } catch (IOException e) {
            e.printStackTrace();
            // throw new CustomException("some IO file issue","500");
        }
        return files;
    }

    private String getEdmType(String type) {
        switch (type){
            case "Boolean":
                return "Edm.Boolean";
            case "Integer":
                return "Edm.Int32";
            case "Double":
                return "Edm.Double";
            case "Long":
                return "Edm.Int64";
            case "Float":
                return "Edm.Single";
            case "Date":
                return "Edm.Date";
            case "DateTime":
                return "Edm.DateTimeOffset";
            default:
                return "Edm.String";
        }

    }


    private JSONObject generateComplexTypes(List<String> complexList) throws CustomException {
        JSONObject complexTypeObj = new JSONObject();
        for(String complexElement : complexList) {
            try {
                String metaContent = IOUtils.toString(
                        getClass().getClassLoader().getResourceAsStream("meta/datamodel/" + complexElement + ".meta"),
                        StandardCharsets.UTF_8
                );
                JSONArray metaArray = JsonPath.parse(metaContent).read("$.*");
                JSONObject nameObj = new JSONObject();
                for(int i =0; i < metaArray.size(); i++) {

                    String name = (String) ((LinkedHashMap) metaArray.get(i)).get("name");
                    if(!name.equals("_id")) {
                        JSONObject typeObj = new JSONObject();
                        if((boolean) ((LinkedHashMap) metaArray.get(i)).get("isPrimitive")) {
                            String type;
                            if((boolean) ((LinkedHashMap) metaArray.get(i)).get("isCollection")) {
                                type = "Collection(" + getEdmType((String) ((LinkedHashMap) metaArray.get(i)).get("type")) + ")";
                            } else {
                                type = getEdmType((String) ((LinkedHashMap) metaArray.get(i)).get("type"));
                            }
                            typeObj.put("type", type);
                        } else {
                            //TODO:All non primitve are now considered as Collection, need to check NavigationProperty
                            String type = "Collection(io.intelliflow.generated.models." +((String) ((LinkedHashMap) metaArray.get(i)).get("type")) + ")";
                            typeObj.put("type", type);
                        }
                        nameObj.put(name, typeObj);
                    }
                }
                complexTypeObj.put(complexElement, nameObj);
            } catch (IOException e) {
                e.printStackTrace();
                // throw new CustomException("some IO file issue","500");
            }
        }
        return complexTypeObj;
    }
}
