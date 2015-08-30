package com.unionbigdata.kafka.loader;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by cqyua on 2015/8/26.
 */
public class MainLoader {

    private final File conf = new File("./conf/loader.conf");
    private final File libs = new File("./libs/");
    private final File pid = new File("./pid");

    private final Logger logger = LogManager.getLogger("MainLoader");
    private final Properties props = new Properties();
    private ExecutorService executor = null;
    private SpecificLoader loader = null;

    public void start(){
       checkAndInit();
        final String loaderClass = props.getProperty("loader.source.type");
        final String topic = props.getProperty("kafka.topic");

        //create the specific loader and init it.
        try {
            loader = (SpecificLoader) Class.forName(loaderClass).newInstance();
        } catch (Exception e) {
            logger.error("Get the instance of " +loaderClass + " failed. " + e);
            this.shutdown();
        }
        try {
            loader.init(props);
        } catch (Exception e) {
            logger.error("Init loader " + loaderClass + " failed." + e);
            this.shutdown();
        }

        int threads = props.getProperty("loader.consumer.thread.num")==null?1:Integer.parseInt(props.getProperty("loader.consumer.thread.num"));
        if (threads <= 0) threads = 1;
        executor = Executors.newFixedThreadPool(threads);

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String,Integer> topics = new HashMap<String, Integer>();
        topics.put(topic,threads);
        Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap = consumer.createMessageStreams(topics);
        List<KafkaStream<byte[],byte[]>> streams = consumerMap.get(topic);
        for (KafkaStream<byte[],byte[]> stream : streams){
            executor.submit(new PartitionConsumer(stream,loader,topic,logger));
        }

    }

    private void checkAndInit(){

        if (pid.exists()){
            logger.error("Pid file exists.");
            this.shutdown();
        }else {
            try {
                if(!pid.createNewFile()){
                    throw new IOException("Create pid file failed.");
                }
                //TODO write the pid into the pid file.
                pid.deleteOnExit();
            } catch (IOException e) {
                logger.error("Create pid file failed," + e);
                this.shutdown();
            }
        }

        //create conf file and folder
        if (!conf.exists()){
            logger.info("config file not found,creates one at " + conf.getAbsolutePath());
            if (!conf.getParentFile().exists()){
                conf.getParentFile().mkdir();
            }
            try {
                conf.createNewFile();
            } catch (IOException e) {
                logger.error("Create config file failed." + e);
            }
            this.shutdown();
        }

        //create the lib folder
        if (!libs.exists()){
            logger.info("Library folder not found ,create one at " + libs.getAbsolutePath());
            libs.mkdirs();
        }
        try {
            props.load(new FileInputStream(conf));
        } catch (IOException e) {
            logger.error("Loading config file failed." + e);
            this.shutdown();
        }
    }

    public void shutdown(){
        if (executor != null){
            executor.shutdown();
        }
        if (loader != null){
            loader.shutdown();
        }
    }

    public static void main(String[] args) {
        MainLoader loader = new MainLoader();
        loader.start();
    }

    private class PartitionConsumer implements Runnable{

        private final KafkaStream<byte[],byte[]> stream;
        private final SpecificLoader loader ;
        private final String topic;
        private final Logger logger;

        PartitionConsumer(KafkaStream<byte[],byte[]> stream,SpecificLoader loader,String topic,Logger logger){
            this.stream = stream;
            this.loader = loader;
            this.topic = topic;
            this.logger = logger;
        }

        public void run() {
            logger.info("PartitionConsumer " + Thread.currentThread() + " started.");
            ConsumerIterator<byte[],byte[]> iterator = stream.iterator();
            while (iterator.hasNext()){
                loader.load(iterator.next().message(),topic);
            }
        }

        public void shutdown(){
            logger.info("PartitionConsumer " + Thread.currentThread() + " shutdown.");
        }
    }
}
