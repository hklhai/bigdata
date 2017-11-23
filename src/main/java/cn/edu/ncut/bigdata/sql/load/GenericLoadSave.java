package cn.edu.ncut.bigdata.sql.load;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 通用的load和save操作
 * Created by Ocean lin on 2017/11/23.
 */
public class GenericLoadSave {

    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("GenericLoadSave").setMaster("local"));
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame userFD = sqlContext.read().load("D://spark//dataframe//users.parquet");
        userFD.printSchema();
        userFD.show();
        // |  name|favorite_color|favorite_numbers|
        userFD.select(userFD.col("name"), userFD.col("favorite_color"))
                .write().save("D://spark//dataframe//save//usersave.parquet");
        sc.close();
    }
}
