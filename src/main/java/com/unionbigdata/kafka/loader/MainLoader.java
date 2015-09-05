package com.unionbigdata.kafka.loader;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.Loader;

import java.awt.peer.ScrollbarPeer;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by cqyua on 2015/8/26.
 */
public class MainLoader {

    private final String confPath = "./conf/loader.conf";
    private final File pid = new File("./pid");

    private final Logger logger = LogManager.getLogger("MainLoader");
    private final Configuration conf = new Configuration();
    private final Map<String, List<SpecificLoader>> topicLoaders = new HashMap<>(); //map from topic to loaders.
    private final Map<String, Integer> topicThreads = new HashMap<>();
    private final Map<String,ExecutorService> topicExecutors = new HashMap<>();

    private ConsumerConnector kafkaConnector = null;
    private LoaderContext context = null;

    private void init() {
        // init the config
        try {
            conf.addResource(confPath);
        } catch (IOException e) {
            logger.warn("Load config resource failed." + e);
        }
        if (conf.getString("group.id","").equals("")){
            conf.getProps().put("group.id",genGroup());
        }
        // init the LoaderContext
        context = new LoaderContext(conf, logger, this, new FailedMessageHandler() {
            @Override
            public void messageFailed(byte[] msg, String topic, Class<?> loaderClss) {
                //TODO
            }
        });

        //init topics and the specific loader.
        String[] topics = conf.getString("loader.topics", "").split(",");
        if (topics.length == 0) {
            logger.warn("No topic config.");
            return;
        }
        for (String topic : topics) {
            int threads = conf.getInt("loader.topic." + topic + ".threads", 1);
            topicThreads.put(topic, threads);
            List<SpecificLoader> loaders = new LinkedList<>();
            topicLoaders.put(topic, loaders);
            String[] dsts = conf.getString("loader.topic." + topic, "").split(",");
            if (dsts.length == 0) {
                logger.info("No dst config for topic:" + topic);
                continue;
            }
            for (String dst : dsts) {
                String clazz = conf.getString("loader.topic." + topic + "." + dst + ".class", "");
                try {
                    SpecificLoader loader = (SpecificLoader) Class.forName(clazz).newInstance();
                    loader.init(context,topic);
                    loaders.add(loader);
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    //this should not happen.
                    logger.error("Create instance of SpecificLoader failed.CLass :" + clazz + e);
                }catch (Exception e){
                    logger.warn("Init loader failed.Loader " + clazz +  " Exception: " +e);
                }
            }
        }
        for (Map.Entry<String,Integer> entry:topicThreads.entrySet()){
            ExecutorService executors = Executors.newFixedThreadPool(entry.getValue());
            topicExecutors.put(entry.getKey(),executors);
        }
    }

    public void start() {
        init();
        kafkaConnector = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(conf.getProps()));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = kafkaConnector.createMessageStreams(topicThreads);
        for (String topic : topicThreads.keySet()) {
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
            for (KafkaStream<byte[], byte[]> stream : streams) {
                topicExecutors.get(topic).submit(new PartitionConsumer(stream, topicLoaders.get(topic), topic, logger));
            }
        }
    }

    private void checkAndCreatePid() throws Exception {

        if (pid.exists()) {
            throw new Exception("Pid file exists.");
        } else {
            if (!pid.createNewFile()) {
                throw new IOException("Create pid file failed.");
            }
            //TODO write the pid into the pid file.
            pid.deleteOnExit();
        }
    }

    public void shutdown() {
        kafkaConnector.shutdown();
        for (ExecutorService executor : topicExecutors.values()){
            executor.shutdown();
        }
        for (List<SpecificLoader> loaders : topicLoaders.values()){
            for (SpecificLoader loader : loaders){
                loader.shutdown();
            }
        }
    }

    //Generate a kakfa group name,it should guarantee the name is unique.
    private String genGroup() {
        return System.currentTimeMillis() + "_" + Thread.currentThread().hashCode();
    }

    public static void main(String[] args) {
        MainLoader loader = new MainLoader();
        loader.start();
    }

    private class PartitionConsumer implements Runnable {

        private final KafkaStream<byte[], byte[]> stream;
        private final List<SpecificLoader> loaders;
        private final String topic;
        private final Logger logger;

        PartitionConsumer(KafkaStream<byte[], byte[]> stream, List<SpecificLoader> loaders, String topic, Logger logger) {
            this.stream = stream;
            this.loaders = loaders;
            this.topic = topic;
            this.logger = logger;
        }

        public void run() {
            logger.info("PartitionConsumer " + Thread.currentThread() + " started.");
            ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
            while (iterator.hasNext()) {
                for (SpecificLoader loader : loaders) {
                    try {
                        loader.load(iterator.next().message());
                    } catch (Exception e) {
                        //TODO handle failed message.
                    }
                }
            }
        }

        public void shutdown() {
            logger.info("PartitionConsumer " + Thread.currentThread() + " shutdown.");
        }
    }

}