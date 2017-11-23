package cn.edu.ncut.bigdata.sql.load;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 手动指定数据源类型
 * <p>
 * Created by Ocean lin on 2017/11/23.
 */
public class SpecifyOption {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("SpecifyOption").setMaster("local"));
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df  = sqlContext.read().format("json").load("D://spark//dataframe//people.json");
        df.select(df.col("name"),df.col("age")).
                write().format("parquet").save("D://spark//dataframe//people_java");

        sc.close();
    }
}
