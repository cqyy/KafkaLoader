package com.unionbigdata.kafka.loader.rest;

import com.unionbigdata.kafka.loader.common.LoaderContext;
import org.glassfish.hk2.utilities.binding.AbstractBinder;


/**
 * Created by kali on 2015/9/8.
 */
public class ServiceBinder extends AbstractBinder {

    private LoaderContext context;

    public ServiceBinder(LoaderContext context){
        this.context = context;
    }

    @Override
    protected void configure() {
        bind(context).to(LoaderContext.class);
    }
}
