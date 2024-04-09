package io.intelliflow.utils;

 /*
    @author rahul.malawadkar@intelliflow.ai
    @created on 20-05-2023
 */

import javax.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class ObjectTransformer {

    public Map<String,Object> transform(Object source,String dataModelVariable) {
        Map<String,Object>map = new HashMap<>();
        map.put(dataModelVariable,source);
        return map;
    }
}
