package com.unionbigdata.kafka.loader;

import java.util.Properties;

/**
 * Created by cqyua on 2015/8/26.
 */
public class ConsoleLoader implements SpecificLoader {

    private String topic = "";

    public void init(Properties props,String topic) {
        //do nothing
        this.topic = topic;
    }


    public void load(byte[] msg) {
            //just print all msg to the console
        System.out.println(topic + " : " + new String(msg));
    }

    public void shutdown() {
        //do nothing
    }
}
