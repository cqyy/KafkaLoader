package com.unionbigdata.kafka.loader;

import java.util.Properties;

/**
 * Created by cqyua on 2015/8/26.
 */
public class ConsoleLoader implements SpecificLoader {
    public void init(Properties props) {
        //do nothing
    }


    public void load(byte[] msg, String topic) {
            //just print all msg to the console
        System.out.println(topic + " : " + new String(msg));
    }

    public void close() {
        //do nothing
    }
}
