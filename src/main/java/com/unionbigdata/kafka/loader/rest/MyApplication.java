package com.unionbigdata.kafka.loader.rest;

import org.glassfish.jersey.server.ResourceConfig;

/**
 * Created by kali on 2015/9/7.
 */
public class MyApplication extends ResourceConfig {


    public MyApplication(){
        register(new ServiceBinder());
        register(LoaderOperation.class);
    }
}
