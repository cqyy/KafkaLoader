package com.unionbigdata.kafka.loader.rest;

import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;

/**
 * Created by kali on 2015/9/8.
 */
public class HelloFactory implements Factory<RestServer.HelloTest> {

    private final RestServer.HelloTest test;

    @Inject
    public HelloFactory(RestServer.HelloTest test){
        this.test = test;
    }

    @Override
    public RestServer.HelloTest provide() {
        return test;
    }

    @Override
    public void dispose(RestServer.HelloTest helloTest) {
        //do nothing
    }
}
