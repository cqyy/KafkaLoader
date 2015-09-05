package com.unionbigdata.kafka.loader;

/**
 * Created by kali on 2015/9/4.
 */
public interface FailedMessageHandler {
    public void messageFailed(byte[] msg,String topic,Class<?> loaderClss);
}
