package com.unionbigdata.kafka.loader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by kali on 2015/8/31.
 */
public class Configuration {

    private final Properties props = new Properties();

    public void addResource(String filename) throws IOException{
        File file = new File(filename);
        props.load(new FileInputStream(file));
    }

    public void addResource(InputStream is) throws IOException {
        props.load(is);
    }

    public int getInt(String key,int defaultValue){
        String strValue = getTrimed(key);
        if (strValue == null){
            return defaultValue;
        }
        try{
            int intValue = Integer.parseInt(strValue);
            return intValue;
        }catch (NumberFormatException e){
            return defaultValue;
        }
    }

    public long getLong(String key,long defaultValue){
        String strValue = getTrimed(key);
        if (strValue == null){
            return defaultValue;
        }
        try{
            long longValue = Long.parseLong(strValue);
            return longValue;
        }catch (NumberFormatException e){
            return defaultValue;
        }
    };

    public float getFloat(String key,float defaultValue){
        String strValue = getTrimed(key);
        if (strValue == null){
            return defaultValue;
        }
        try{
            float floatValue = Float.parseFloat(strValue);
            return floatValue;
        }catch (NumberFormatException e){
            return defaultValue;
        }
    }

    public String getString(String key,String defaultValue){
        String strValue = getTrimed(key);
        if (strValue == null){
            return defaultValue;
        }
        return strValue;
    }

    public boolean getBoolean(String key,boolean defaultValue){
        String strValue = getTrimed(key);
        if ("true".equals(strValue)){
            return true;
        }else if ("false".equals(strValue)){
            return false;
        }
        return defaultValue;
    }

    public Class<?> getClass(String key,Class<?> defaultValue){
        String strValue = getTrimed(key);
        if (strValue == null){
            return defaultValue;
        }
        try{
            Class<?> clazz = Class.forName(strValue);
        } catch (ClassNotFoundException e) {
           return defaultValue;
        }
    }

    private String getTrimed(String key){
        String val = props.getProperty(key);
        return val != null ?val.trim():null;
    }


}
