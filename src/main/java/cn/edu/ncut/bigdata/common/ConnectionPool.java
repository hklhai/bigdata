package cn.edu.ncut.bigdata.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * 简版连接池
 * Created by Ocean lin on 2017/11/28.
 */
public class ConnectionPool {


    // 静态的Connection队列
    private static LinkedList<Connection> connectionQueue;

    // 加载驱动
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    // 获取连接，多线程访问并发控制
    public synchronized static Connection getConnection() {
        try {
            if (null == connectionQueue) {
                connectionQueue = new LinkedList<>();
                for (int i = 0; i < 10; i++) {
                    Connection connection = DriverManager.getConnection("jdbc:mysql://spark01:3306/hivedb", "", "");
                    connectionQueue.push(connection);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    // 释放连接
    public static void returnConnection(Connection connection) {
        connectionQueue.push(connection);
    }

}
