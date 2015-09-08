package com.unionbigdata.kafka.loader.rest;

import org.glassfish.hk2.utilities.binding.AbstractBinder;

/**
 * Created by kali on 2015/9/8.
 */
public class ServiceBinder extends AbstractBinder {
    @Override
    protected void configure() {
        bindFactory(HelloFactory.class).to(RestServer.HelloTest.class);
    }
}
