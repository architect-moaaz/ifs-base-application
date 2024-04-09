package io.intelliflow.service;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Singleton
@RegisterRestClient(configKey = "key-vault-api")
public interface KeyVaultService {

    @GET
    @Path("/getSecrete")
    @Produces(MediaType.APPLICATION_JSON)
    String getData(@HeaderParam("workspace") String workSpace,
                     @HeaderParam("appname") String appName,
                     @HeaderParam("datakey") String dataKey);
}
