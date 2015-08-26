package com.unionbigdata.kafka.loader;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by cqyua on 2015/8/26.
 */
public class MainLoader {

    public static final File conf = new File("./conf/loader.conf");
    public static final File libs = new File("./libs/");

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, IllegalAccessException, InstantiationException {
        //create conf file and folder
        if (!conf.exists()){
            if (!conf.getParentFile().exists()){
                conf.getParentFile().mkdir();
            }
            conf.createNewFile();
        }

        //create the lib folder
        if (!libs.exists()){
            libs.mkdirs();
        }

        Properties props = new Properties();
        props.load(new FileInputStream(conf));

        final String loaderClass = props.getProperty("loader.source.type");
        final String topic = props.getProperty("kafka.topic");
        //final String group = props.getProperty("kafka.group");


        SpecificLoader loader = (SpecificLoader) Class.forName(loaderClass).newInstance();
        loader.init(props);

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String,Integer> topics = new HashMap<String, Integer>();
        topics.put(topic,new Integer(1)); //TODO change the thread number to be configed
        Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap = consumer.createMessageStreams(topics);
        List<KafkaStream<byte[],byte[]>> streams = consumerMap.get(topic);
        for (KafkaStream<byte[],byte[]> stream : streams){
            ConsumerIterator<byte[],byte[]> iterator = stream.iterator();
            while (iterator.hasNext()){
                loader.load(iterator.next().message(),topic);
            }
        }
    }
}
