package com.byd.utils;

import java.net.URL;
import java.text.MessageFormat;

/**
 * @author wang.yonggui1
 * @date 2021-11-10 9:37
 */
public class PathTools {

    /**
     * get resource file path
     * @param fileName
     * @return
     */
    public static String getPath(String fileName) {
        if (null == fileName || fileName.length() == 0) {
            return null;
        }
        //兼容abs-path
        if (fileName.startsWith("/")){
            return fileName;
        }
        //兼容win abspath
        if (fileName.contains(":")){
            return fileName;
        }
        URL resource = currentLoader().getResource(fileName);
        if (null==resource){
            throw new RuntimeException(MessageFormat
                    .format("check if exist resource file: {0}", fileName));
        }
        return resource.getPath();
    }

    public static ClassLoader currentLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    public static void main(String[] args) {
        System.out.println(getPath("kerberos/ic.dev.hdfs.keytab"));
    }

}
