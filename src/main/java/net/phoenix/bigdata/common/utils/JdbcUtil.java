package net.phoenix.bigdata.common.utils;

import net.phoenix.bigdata.common.config.GlobalConfig;

import java.sql.*;

/**
 * jdbc通用的方法
 * @Date: 2019/3/4 下午6:00
 *
 */
public class JdbcUtil {
    //url
    private static String url = GlobalConfig.SYS_DB_URL;
    //user
    private static String user = GlobalConfig.SYS_DB_USER;
    //password
    private static String password = GlobalConfig.SYS_DB_PWD;
    //驱动程序类
    private static String driverClass = GlobalConfig.MYSQL_DRIVER_CLASS;
    /**
     * 只注册一次，静态代码块
     */
    static{

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }



    /**
     * 获取连接方法
     */
    public static Connection getConnection(){
        try {
            Connection conn = DriverManager.getConnection(url, user, password);
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 释放资源的方法
     */
    public static void close(Statement stmt,Connection conn){
        if(stmt!=null){
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 释放资源的方法
     */
    public static void close(ResultSet rs,Statement stmt,Connection conn){
        if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        if(stmt!=null){
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(JdbcUtil.getConnection());
    }

}

