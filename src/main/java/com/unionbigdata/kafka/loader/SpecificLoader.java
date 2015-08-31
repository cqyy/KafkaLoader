package com.unionbigdata.kafka.loader;

import java.util.Properties;

/**
 * Created by cqyua on 2015/8/26.
 */
public interface SpecificLoader {

    /**
     * Init specific loader using the configs.
     * @param props configs to init the specific loader.
     * @param topic message topic
     */
    void init(Properties props,String topic) throws Exception;

    /**
     * Load message from kafka to specific storage etc.
     * @param msg message to load

     */
    void load(byte[] msg);

    /**
     * Close the loader,the loader should flush all data out if it has caches.
     */
    void shutdown();
}
