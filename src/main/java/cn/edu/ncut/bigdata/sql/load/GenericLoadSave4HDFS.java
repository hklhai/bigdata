package cn.edu.ncut.bigdata.sql.load;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 通用的load和save操作 for HDFS
 * Created by Ocean lin on 2017/11/23.
 */
public class GenericLoadSave4HDFS {

    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("GenericLoadSave4HDFS").setMaster("local"));
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame userFD = sqlContext.read().load("hdfs://spark01:9000/sql-load/users.parquet");
        userFD.printSchema();
        userFD.show();
        // |  name|favorite_color|favorite_numbers|
        userFD.select(userFD.col("name"), userFD.col("favorite_color"))
                .write().save("hdfs://spark01:9000/sql-load/nameAndFavorite.parquet");
        sc.close();
    }
}
