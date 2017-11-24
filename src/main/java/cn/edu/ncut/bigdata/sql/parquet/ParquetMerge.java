package cn.edu.ncut.bigdata.sql.parquet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by Ocean lin on 2017/11/23.
 */
public class ParquetMerge {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("ParquetMerge"));
        SQLContext sqlContext = new SQLContext(sc);

    }
}
