package net.phoenix.bigdata.common.utils;


import lombok.extern.slf4j.Slf4j;
import net.phoenix.bigdata.common.Constant;

import java.io.*;
import java.util.Properties;

@Slf4j
public class PropertiesUtils {
    private static Properties properties;
    static{

    }
    public static Properties getDefaultProperties(){
        properties = new Properties();
        InputStream in = PropertiesUtils.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            properties.load(in);
            in.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return properties;
    }


    public static Properties getProperties(String filePath) throws IOException {
        Properties properties_file = new Properties();
        BufferedReader br = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath),"utf-8"));
        properties_file.load(br);
        return properties_file;
    }

    public static void main(String[] args) {
        String property = PropertiesUtils.getDefaultProperties().getProperty("hbase.zk.host");
        System.out.println(property);

    }
}
