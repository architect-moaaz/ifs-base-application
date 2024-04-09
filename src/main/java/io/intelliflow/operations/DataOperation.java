package io.intelliflow.operations;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import javax.enterprise.context.ApplicationScoped;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.minidev.json.JSONObject;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.intelliflow.generated.models.*;


@ApplicationScoped
public class DataOperation {
    
    private final HttpClient httpClient = HttpClient.newBuilder()
    .version(HttpClient.Version.HTTP_2)
    .build();

    public void insertData(Object obj,String appName) throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException{
        //String appName="Operations48";
        JSONObject mapObj = new JSONObject();
        for (Field field : obj.getClass().getDeclaredFields()) {
            field.setAccessible(true); // You might want to set modifier to public first.
            Object value = field.get(obj); 
            if (value != null) {
                System.out.println(field.getName() + "=" + value);
                mapObj.put(field.getName(), value);

            }
        }
        
        HttpRequest postRequest = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString(String.valueOf(mapObj)))
        .setHeader("Content-Type", "application/json")
        .uri(URI.create("http://151.106.38.94:31513/query/Intelliflow/"+appName+"/"+obj.getClass().getSimpleName()))
        .build();
        HttpResponse<String> response = httpClient.send(postRequest, HttpResponse.BodyHandlers.ofString());
        


    }
    public void updateDetails(Object obj,String appName) throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException{
        
        JSONObject mapObj = new JSONObject();
        String id="";
        for (Field field : obj.getClass().getDeclaredFields()) {
            field.setAccessible(true); // You might want to set modifier to public first.
            Object value = field.get(obj); 
           if (value != null) {
            System.out.println(field.getName() + "=" + value);

            if(field.getName()=="_id")
            {
                id=String.valueOf(value);
            }
            else{
                mapObj.put(field.getName(), value);
            }
              

            }
        }
        
        HttpRequest postRequest = HttpRequest.newBuilder()
        .method("PATCH", HttpRequest.BodyPublishers.ofString(String.valueOf(mapObj)))
        .setHeader("Content-Type", "application/json")
        .uri(URI.create("http://151.106.38.94:31513/query/Intelliflow/"+appName+"/"+obj.getClass().getSimpleName()+"('"+id+"')"))
        .build();
        HttpResponse<String> response = httpClient.send(postRequest, HttpResponse.BodyHandlers.ofString());
        

    }

    public Object fetchDetails(Object obj,String appName) throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException{
        String query="";
        Gson gson = new Gson();
        JSONObject mapObj = new JSONObject();
        int i=0;

        for (Field field : obj.getClass().getDeclaredFields()) {
            field.setAccessible(true); // You might want to set modifier to public first.
            
            Object value = field.get(obj); 
            if (value != null && field.getName() !="serialVersionUID") {
                System.out.println("type "+field.getType()+" string: "+String.class);
                if(i==0)
                {
                    query+="&$filter="+field.getName()+"+eq+";
                    if (field.getType().equals(String.class)) {
                        query+="'"+value+"'";
                      }else
                      {
                        query+=value;
                      }
                }else{
                    query+=" and "+field.getName()+"+eq+";
                    if (field.getType().equals(String.class)) {
                        query+="'"+value+"'";
                      }else
                      {
                        query+=value;
                      }
                }
                mapObj.put(field.getName(), value);
                i++;
            }

            }
           
       // query


    
       String params = URLEncoder.encode(query, "UTF-8");
       String Url = "http://151.106.38.94:31513/query/Intelliflow/"+appName+"/"+obj.getClass().getSimpleName()+"?" + query;
       System.out.println("URL:: "+Url);

        HttpRequest postRequest = HttpRequest.newBuilder()
        .GET()
        .setHeader("Content-Type", "application/json")
        .uri(URI.create(Url))
        .build();
        HttpResponse<String> response = httpClient.send(postRequest, HttpResponse.BodyHandlers.ofString());
       
        JsonParser parser = new JsonParser();
		JsonObject JSONObject = parser.parse(response.body().toString()).getAsJsonObject();
        JsonArray arr=JSONObject.getAsJsonArray("value");
        
      
        if(arr.size()>0)
        {
            return gson.fromJson(arr.get(0).toString(),obj.getClass());
        }
        else
        {
            return null;
        }
      


        // create a JSON from above and call ODATA Api


    }
  

}
