package io.intelliflow.service;

import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

/*
    @author rahul.malawadkar@intelliflow.ai
    @created on 18-05-2023
 */

@Singleton
@RegisterRestClient(configKey = "router-api")
public interface RouterService {

    @GET
    @Path("app-center/{workspace}/{appname}/context")
    @Produces(MediaType.APPLICATION_JSON)
    String getContext(@PathParam("workspace") String workspace,
                      @PathParam("appname") String appName,
                      @HeaderParam("Devicesupport") String deviceSupport);

    @POST
    @Path("/q/{workspace}/{appname}/{processname}")
    @Produces(MediaType.APPLICATION_JSON)
    String startProcess(
            @PathParam("workspace") String workspace,
            @PathParam("appname") String appName,
            @PathParam("processname") String processName,
            @QueryParam("user") String userQuery,
            @HeaderParam("user") String userHeader,
            @QueryParam("actionCategory") String actionCategory,
            @RequestBody Object requestBody
    );

}