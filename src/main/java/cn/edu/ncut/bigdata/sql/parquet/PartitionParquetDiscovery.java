package cn.edu.ncut.bigdata.sql.parquet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * parquet自动推断分区
 * Created by Ocean lin on 2017/11/23.
 */
public class PartitionParquetDiscovery {

    public static void main(String[] args) {
//        hadoop fs -mkdir /spark-study/users
//        hadoop fs -mkdir /spark-study/users/gender=male
//        hadoop fs -mkdir /spark-study/users/gender=male/country=CN
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("PartitionParquetDiscovery"));
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().parquet("hdfs://spark01:9000//spark-study/users/gender=male/country=CN/users.parquet");
        df.printSchema();
        df.show();
    }
}
