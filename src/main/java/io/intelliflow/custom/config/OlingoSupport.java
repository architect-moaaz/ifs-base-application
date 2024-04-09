package io.intelliflow.custom.config;

import org.jbpm.workflow.core.impl.DataDefinition;
import org.jbpm.workflow.core.impl.IOSpecification;
import org.jbpm.workflow.core.node.WorkItemNode;
import org.kie.kogito.internal.process.runtime.KogitoWorkItem;

import java.util.List;

public class OlingoSupport {

    public static String getOutputVariable(KogitoWorkItem workItem, String fullyQualifiedName) {
        WorkItemNode node =(WorkItemNode) workItem.getNodeInstance().getNode();
        IOSpecification ioSpec = node.getIoSpecification();
        for(DataDefinition dataDefinition : ioSpec.getDataOutputs()){
            if(dataDefinition.getType().equals(fullyQualifiedName)){
                return dataDefinition.getLabel();
            }
        }
        return "result";
    }
}
