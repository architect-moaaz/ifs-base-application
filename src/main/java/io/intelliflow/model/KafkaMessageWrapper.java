package io.intelliflow.model;

import java.io.Serializable;

public class KafkaMessageWrapper<T> implements Serializable {

    public T data;

    public String workspace;

    public String allowList;

    public String sender;

    public String initiatedBy;

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getInitiatedBy() {
        return initiatedBy;
    }

    public void setInitiatedBy(String initiatedBy) {
        this.initiatedBy = initiatedBy;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getWorkspace() {
        return workspace;
    }

    public void setWorkspace(String workspace) {
        this.workspace = workspace;
    }

    public String getAllowList() {
        return allowList;
    }

    public void setAllowList(String allowList) {
        this.allowList = allowList;
    }
}
