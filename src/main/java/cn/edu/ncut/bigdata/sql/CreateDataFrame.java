package cn.edu.ncut.bigdata.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 使用Json文件创建DataFrame
 * Created by Ocean lin on 2017/11/22.
 */
public class CreateDataFrame {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CreateDataFrame");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame dataFrame = sqlContext.read().json("hdfs://spark01:9000/students.json");
        dataFrame.show();
        sc.close();
    }

}
