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
     * @throws Exception
     */
    void init(Properties props,String topic) throws Exception;

    /**
     * Load message from kafka to specific storage etc.
     * This method may invoked by multi threads.
     * @param msg message to load
     */
    void load(byte[] msg) throws Exception;

    /**
     * Close the loader,the loader should flush all data out if it has caches.
     */
    void shutdown();
}
