package net.phoenix.bigdata.common.config;

import net.phoenix.bigdata.common.Constant;
import net.phoenix.bigdata.common.utils.PropertiesUtils;

import java.io.Serializable;

/**
 * 在生产上一般通过配置中心来管理
 */
public class GlobalConfig implements Serializable {
    /**
     * mysql数据库driver class
     */
    public static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    /**
     * 数据库jdbc url
     */
    public static final String SYS_DB_URL = "jdbc:mysql://"
            +PropertiesUtils.getDefaultProperties().getProperty(Constant.SYS_DB_HOST)+Constant.COLON
            +PropertiesUtils.getDefaultProperties().getProperty(Constant.SYS_DB_PORT)+"/"
            +PropertiesUtils.getDefaultProperties().getProperty(Constant.SYS_DB_DBNAME)+
            "?useUnicode=true&characterEncoding=utf8";
    /**
     * 数据库user name
     */
    public static final String SYS_DB_USER = PropertiesUtils.getDefaultProperties().getProperty(Constant.SYS_DB_USER);
    /**
     * 数据库password
     */
    public static final String SYS_DB_PWD = PropertiesUtils.getDefaultProperties().getProperty(Constant.SYS_DB_PWD);
    /**
     * 批量提交size
     */
    public static final int BATCH_SIZE = 2;

    //HBase相关配置
    public static final String HBASE_ZK_HOST = PropertiesUtils.getDefaultProperties().getProperty(Constant.HBASE_ZK_HOST);
    public static final String HBASE_ZK_PORT = PropertiesUtils.getDefaultProperties().getProperty(Constant.HBASE_ZK_PORT);
    public static final String HBASE_ZK_PATH = PropertiesUtils.getDefaultProperties().getProperty(Constant.HBASE_ZK_PATH);

    //Kafka相关配置
    public static final String KAFKA_SERVERS = PropertiesUtils.getDefaultProperties().getProperty(Constant.KAFKA_SERVERS);
    public static final String KAFKA_ZK_CONNECTS = PropertiesUtils.getDefaultProperties().getProperty(Constant.KAFKA_ZK_CONNECTS);
    public static final String KAFKA_GROUP_ID = PropertiesUtils.getDefaultProperties().getProperty(Constant.KAFKA_GROUP_ID);


    public static void main(String[] args) {
        System.out.println(GlobalConfig.SYS_DB_URL);

    }
}
