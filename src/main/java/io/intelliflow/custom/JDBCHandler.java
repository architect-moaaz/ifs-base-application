package io.intelliflow.custom;

 /*
    @author rahul
    @created on 03-02-2023
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import io.intelliflow.service.KeyVaultService;
import io.quarkus.logging.Log;
import org.jbpm.workflow.core.impl.DataDefinition;
import org.jbpm.workflow.core.impl.IOSpecification;
import org.jbpm.workflow.core.node.WorkItemNode;
import org.kie.kogito.internal.process.runtime.KogitoWorkItem;
import org.kie.kogito.internal.process.runtime.KogitoWorkItemHandler;
import org.kie.kogito.internal.process.runtime.KogitoWorkItemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;


public class JDBCHandler implements KogitoWorkItemHandler {
    private static final Logger logger = LoggerFactory.getLogger(JDBCHandler.class);
    private static final String RESULT = "Result";
    private static final int DEFAULT_MAX_RESULTS = 10;
    private String workspace;

    private String appName;
    private final KeyVaultService keyVaultService;

    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("application.properties");

    public JDBCHandler(KeyVaultService keyVaultService) {
        this.keyVaultService = keyVaultService;
        try {
            Properties appProps = new Properties();
            appProps.load(inputStream);
            workspace = appProps.get("ifs.app.workspace").toString();
            appName = appProps.get("ifs.app.miniappname").toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void executeWorkItem(KogitoWorkItem workItem, KogitoWorkItemManager manager) {
        Log.info("JDBC Operations Begins!!");
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Map<String, Object> results = new HashMap<>();
        try {
            String sqlStatement = (String) workItem.getParameter("SQLStatement");
            String maxResultsInput = (String) workItem.getParameter("MaxResults");
            String dataKey = (String) workItem.getParameter("dataKey");
            int maxResults = maxResultsInput != null && !maxResultsInput.trim().isEmpty() ? Integer.parseInt(maxResultsInput) : DEFAULT_MAX_RESULTS;
            try {
                connection = createDBConnection(dataKey);
                statement = connection.prepareStatement(sqlStatement);
                statement.setMaxRows(maxResults);
                boolean containsResultSet = statement.execute();
                WorkItemNode node = (WorkItemNode) workItem.getNodeInstance().getNode();
                IOSpecification ioSpec = node.getIoSpecification();
                DataDefinition dataDefinition = ioSpec.getDataOutputs().get(0);
                Class<?> clazz = null;
                for (DataDefinition dDef : ioSpec.getDataOutputs()) {
                    if (RESULT.equals(dDef.getLabel())) {
                        clazz = Class.forName(dataDefinition.getType());
                    }
                }
                if (containsResultSet) {
                    resultSet = statement.getResultSet();
                    List<Object>list = processResults(resultSet,clazz);
                    if(!list.isEmpty()) {
                        Object data = list.get(0);
                        results.put(RESULT, data);
                    }
                } else {
                    results.put(RESULT, statement.getUpdateCount());
                }
                manager.completeWorkItem(workItem.getStringId(), results);
            } finally {
                closeResources(connection,statement,resultSet);
            }
        } catch (Exception e) {
            results.put(RESULT, null);
            logger.error(e.getMessage());
        }
    }

    private void closeResources(Connection connection, PreparedStatement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected List<Object> processResults(ResultSet resultSet, Class<?> clazz) throws SQLException {
        if(clazz == null){
            clazz = Object.class;
        }
        List<Object> myList = new ArrayList<>();
        while (resultSet.next()) {
            int columnCount = resultSet.getMetaData().getColumnCount();
            Map<String, String> rowMap = new HashMap<>();
            for (int i = 0; i < columnCount; i++) {
                String columnName = resultSet.getMetaData().getColumnLabel(i + 1);
                String columnValue = resultSet.getString(i + 1);
                rowMap.put(columnName, columnValue);
            }
            String json = new Gson().toJson(rowMap);
            Object object = new Gson().fromJson(json, clazz);
            myList.add(object);
        }
        return myList;
    }

    @Override
    public void abortWorkItem(KogitoWorkItem workItem, KogitoWorkItemManager manager) {
        Log.error("Error happened in the custom work item definition.");
    }

    private Connection createDBConnection(String dataKey) throws SQLException, JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String keyVaultResponse = keyVaultService.getData(workspace, appName, dataKey);
        Map<String, String> keyVaultResponseMap = objectMapper.readValue(keyVaultResponse, new TypeReference<>() {
        });
        Map<String, String> jsonMap = objectMapper.readValue(keyVaultResponseMap.get("data"), new TypeReference<>() {
        });
        String dbName = removeQuotesFromJson(jsonMap.get("dbName"));
        String dbUserName = removeQuotesFromJson(jsonMap.get("dbUserName"));
        String dbPassword = removeQuotesFromJson(jsonMap.get("dbPassword"));
        String dbType = removeQuotesFromJson(jsonMap.get("dbType"));
        String machineAddress = removeQuotesFromJson(jsonMap.get("host"));
        String port = removeQuotesFromJson(jsonMap.get("port"));
        String connectionString = "jdbc:" + dbType + "://" + machineAddress + ":" + port + "/" + dbName + "?useSSL=false";
        Log.info("Connection String: " + connectionString);
        return DriverManager.getConnection(
                connectionString, dbUserName, dbPassword);
    }

    public static String removeQuotesFromJson(String jsonString) {
        return jsonString.replace("\"", "");
    }

}