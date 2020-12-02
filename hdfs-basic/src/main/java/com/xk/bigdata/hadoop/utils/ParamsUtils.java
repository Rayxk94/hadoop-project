package com.xk.bigdata.hadoop.utils;

import java.io.IOException;
import java.util.Properties;

public class ParamsUtils {

    private static Properties properties = new Properties();

    static {
        try {
            properties.load(ParamsUtils.class.getClassLoader().getResourceAsStream("hdfs.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() {
        return properties;
    }

}
