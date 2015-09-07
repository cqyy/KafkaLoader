package com.unionbigdata.kafka.loader.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;

/**
 * Created by kali on 2015/9/6.
 */
@Path("/")
public class LoaderOperation {


    @GET
    @Path("/status")
    @Produces("text/plain")
    public String status(@Context ContainerRequestContext crc){
        RestServer.HelloTest test = (RestServer.HelloTest) crc.getProperty("loader.context");
        return test.hello();
    }
}