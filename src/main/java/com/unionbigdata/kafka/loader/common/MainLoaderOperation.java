package com.unionbigdata.kafka.loader.common;

import java.util.Set;

/**
 * Created by kali on 2015/9/10.
 */
public interface MainLoaderOperation {

    /**
     * Get the kafka topics under loading.
     * @return
     */
    Set<String> topics();

    void shutdown();
}
