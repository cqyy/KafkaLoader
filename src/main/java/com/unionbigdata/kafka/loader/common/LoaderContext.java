package com.unionbigdata.kafka.loader.common;

import com.unionbigdata.kafka.loader.conf.Configuration;
import com.unionbigdata.kafka.loader.core.MainLoader;
import org.apache.log4j.Logger;

/**
 * Created by kali on 2015/9/4.
 */
public class LoaderContext {

    public final Configuration conf;
    public final Logger logger;
    public final MainLoaderOperation loader;
    public final FailedMessageHandler handler;

    public LoaderContext(Configuration conf,Logger logger,MainLoaderOperation loader,FailedMessageHandler handler){
        this.conf = conf;
        this.logger = logger;
        this.loader = loader;
        this.handler = handler;
    }
}
