package io.intelliflow.model;

import java.util.List;

public class AppModel {

    private String workspace;
    private String app;
    private List<ApiPath> paths;

    private String deviceSupport;

    private String appDisplayName;

    public AppModel() {
    }

    public String getWorkspace() {
        return workspace;
    }

    public void setWorkspace(String workspace) {
        this.workspace = workspace;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public List<ApiPath> getPaths() {
        return paths;
    }

    public void setPaths(List<ApiPath> paths) {
        this.paths = paths;
    }

    public String getDeviceSupport() {
        return deviceSupport;
    }

    public void setDeviceSupport(String deviceSupport) {
        this.deviceSupport = deviceSupport;
    }

    public String getAppDisplayName() {
        return appDisplayName;
    }

    public void setAppDisplayName(String appDisplayName) {
        this.appDisplayName = appDisplayName;
    }


    @Override
    public String toString() {
        return "AppModel{" +
                "workspace='" + workspace + '\'' +
                ", app='" + app + '\'' +
                ", appdisplayname='" + appDisplayName + '\'' +
                ", paths=" + paths + '\'' +
                ", deviceSupport =" + deviceSupport +
                '}';
    }
}
