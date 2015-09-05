package com.unionbigdata.kafka.loader.utils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Map;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Created by kali on 2015/9/5.
 */
public class LibClassLoader extends ClassLoader {

    private final Map<String,Class<?>> classes = new TreeMap<>();
    private final File libFolder = new File("./lib");

    private void init(){
        if (!libFolder.exists()||!libFolder.isDirectory()){
            return;
        }
        File[] jars = libFolder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        });
        for(File jar : jars){
            try {
                JarFile file = new JarFile(jar);
                Enumeration ens = file.entries();
                while (ens.hasMoreElements()){
                    JarEntry entry = (JarEntry) ens.nextElement();
                    InputStream is = file.getInputStream(entry);
                    byte[] data = new byte[is.available()];
                    is.read(data,0,data.length);
                    Class<?> clazz = defineClass(entry.getName(),data,0,data.length);
                }
            } catch (IOException e) {
                continue;
            }
        }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {

        Class<?> clazz = classes.get(name);
        if (clazz!=null){
            return clazz;
        }
        throw new ClassNotFoundException(name);
    }
}
