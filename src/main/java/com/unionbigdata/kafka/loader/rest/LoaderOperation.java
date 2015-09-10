package com.unionbigdata.kafka.loader.rest;

import com.unionbigdata.kafka.loader.common.LoaderContext;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.List;
import java.util.Set;

/**
 * Created by kali on 2015/9/6.
 */
@Path("/")
public class LoaderOperation {

    @Inject
    LoaderContext context;

    @GET
    @Path("/status")
    @Produces("text/json")
    public Set<String> topics(){
        return context.loader.topics();
    }

    @GET
    @Path("/shutdown")
    @Produces("text/plain")
    public void shutdown(){
        context.loader.shutdown();
    }
}
