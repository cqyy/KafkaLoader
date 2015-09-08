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

    @Context
    RestServer.HelloTest test;

    @GET
    @Path("/status")
    @Produces("text/plain")
    public String status(@Context ContainerRequestContext crc){
      //t = crc.getProperty("loader.context");
        return test != null ?"OK":"Failed";
    }
}
