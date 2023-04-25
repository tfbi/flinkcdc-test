package com.byd.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author wang.yonggui1
 * @date 2021-10-27 13:54
 */
public class ConfigUtils {

    private static Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    private ConfigUtils() {
    }


    public static String getStrBykey(Properties properties,String key){
        return properties.getProperty(key);
    }

    public static Integer getIntBykey(Map<String,String> properties,String key){
        Integer res=null;
        if (!properties.containsKey(key)){
            return null;
        }
        try {
            res=Integer.valueOf(properties.get(key));
        }catch (Exception e){
            log.error("e:",e);
        }
        return res;
    }

    public static Integer getIntBykey(Properties properties,String key){
        Integer res=null;
        if (!properties.containsKey(key)){
            return null;
        }
        try {
            res=Integer.valueOf(properties.getProperty(key));
        }catch (Exception e){
            log.error("e:",e);
        }
        return res;
    }



    /**
     * prop to map
     * @param properties
     * @return
     */
    public static Map<String,String> prop2Map(Properties properties){
        if (null==properties || properties.size()<=0){
            return null;
        }
        Map<String, String> map = new HashMap<>(8);
        for (String key:properties.stringPropertyNames()){
            map.put(key,properties.getProperty(key));
        }
        return map;
    }

    /**
     * map to prop
     * @param map
     * @return
     */
    public static Properties  mapToProp(Map<String,Object> map){
        if (null==map || map.isEmpty()){
            return null;
        }
        Properties properties = new Properties();
        for (Map.Entry<String,Object> entry:map.entrySet()){
            properties.put(entry.getKey(),entry.getValue());
        }
        return properties;
    }

    /**
     * 外部加载配置
     * @param path
     */
    public static Properties loadProperties(String path){
        return readProperties(path);
    }

    /**
     * read from resource dir
     *
     * @param filePaths
     * @return
     */
    private static Properties readProperties(String... filePaths) {
        Properties properties = new Properties();
        InputStream inputStream = null;
        for (String filePath : filePaths) {
            inputStream = ConfigUtils.class.getClassLoader().getResourceAsStream(filePath);

            if (null==inputStream){
                try {
                    inputStream=new FileInputStream(filePath);
                } catch (FileNotFoundException e) {
                    log.error("load properties error:",e);
                    return null;
                }
            }
            try {
                properties.load(inputStream);
                log.info("load success properties : {}",filePath);
            } catch (IOException e) {
                log.error("load properties error:",e);
                return null;
            }finally {
                if (null!=inputStream){
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }
        return properties;
    }

}
