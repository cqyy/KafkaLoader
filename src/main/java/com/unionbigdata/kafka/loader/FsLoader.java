package com.unionbigdata.kafka.loader;

import java.io.*;

/**
 * Created by kali on 2015/9/1.
 */
public class FsLoader implements SpecificLoader {

    private String path;
    private String filename = "fsloader.data";
    private File file;
    private OutputStream os ;


    @Override
    public void init(LoaderContext context, String topic) throws Exception {
        path = context.conf.getString("loader.topic." + topic + ".dst.fs.path",".");
        file = new File(path,filename);
        if (!file.exists()){
            if (!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }
            file.createNewFile();
        }
        os = new FileOutputStream(file);
    }

    @Override
    public void load(byte[] msg) throws Exception {
        os.write(msg);
        os.write('\n');
    }

    @Override
    public void shutdown() {
        try {
            os.close();
        } catch (IOException e) {
            //TODO
        }
    }
}
