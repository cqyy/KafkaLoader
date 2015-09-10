package com.unionbigdata.kafka.loader.core;

import com.unionbigdata.kafka.loader.common.LoaderContext;
import com.unionbigdata.kafka.loader.common.MainLoaderOperation;
import com.unionbigdata.kafka.loader.common.SpecificLoader;
import com.unionbigdata.kafka.loader.conf.Configuration;
import com.unionbigdata.kafka.loader.common.FailedMessageHandler;
import com.unionbigdata.kafka.loader.rest.RestServer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by cqyua on 2015/8/26.
 */
public class MainLoader implements MainLoaderOperation {

    private final String confPath = "./conf/loader.conf";
    private final File pid = new File("./pid");
    private final File libs = new File("./lib");

    private final Logger logger = LogManager.getLogger("MainLoader");
    private final Configuration conf = new Configuration();
    private final Map<String, List<SpecificLoader>> topicLoaders = new HashMap<>(); //map from topic to loaders.
    private final Map<String, Integer> topicThreads = new HashMap<>();              //map from topic to consumer thread number.
    private final Map<String,ExecutorService> topicExecutors = new HashMap<>();     //map from topic to consumer executor service.

    private ConsumerConnector kafkaConnector = null;
    private LoaderContext context = null;
    private ClassLoader cl = null;
    private RestServer restServer = null;

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

        //init the class loader
        if (!libs.exists()){
            libs.mkdir();
        }
        File[] jars = libs.listFiles();
        URL[] urls = new URL[jars.length];
        for(int i = 0; i < jars.length; i++){
            try {
                urls[i] = jars[i].toURI().toURL();
            } catch (MalformedURLException e) {
                logger.warn("Load jar " + jars[i] + " failed." + e);
            }
        }
        cl = new URLClassLoader(urls);

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
            String[] dsts = conf.getString("loader.topic." + topic + ".dst", "").split(",");
            if (dsts.length == 0) {
                logger.info("No dst config for topic:" + topic);
                continue;
            }
            for (String dst : dsts) {
                String className = conf.getString("loader.topic." + topic + ".dst." + dst + ".class", "");
                try {
                    Class clazz = cl.loadClass(className);
                    Constructor<? extends SpecificLoader> ctor = clazz.getConstructor();
                    SpecificLoader loader = ctor.newInstance();
                    loader.init(context,topic);
                    loaders.add(loader);
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    //this should not happen.
                    logger.error("Create instance of SpecificLoader failed.CLass :" + className + e);
                }catch (Exception e){
                    logger.warn("Init loader failed.Loader " + className + " Exception: " + e);
                }
            }
        }

        //init executor service
        for (Map.Entry<String,Integer> entry:topicThreads.entrySet()){
            ExecutorService executors = Executors.newFixedThreadPool(entry.getValue());
            topicExecutors.put(entry.getKey(),executors);
        }

        //init REST Server
        restServer = new RestServer(context);
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
        try {
            restServer.start();
        } catch (Exception e) {
            logger.error("REST¡¡Server started failed." + e);
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

    @Override
    public Set<String> topics() {
        Set<String> topics = new HashSet<>();
        synchronized (topicLoaders){
            topics.addAll(topicLoaders.keySet());
        }
        return topics;
    }

    @Override
    public void shutdown() {
        restServer.shutdown();
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
                byte[] msg = iterator.next().message();
                for (SpecificLoader loader : loaders) {
                    try {
                        loader.load(msg);
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