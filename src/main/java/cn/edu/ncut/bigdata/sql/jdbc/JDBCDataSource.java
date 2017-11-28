package cn.edu.ncut.bigdata.sql.jdbc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Ocean lin on 2017/11/25.
 */
// TODO: 2017/11/28 待开发 
public class JDBCDataSource {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JDBCDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建HiveContext，注意，这里，它接收的是SparkContext作为参数，不是JavaSparkContext
        HiveContext hiveContext = new HiveContext(sc.sc());


    }
}
