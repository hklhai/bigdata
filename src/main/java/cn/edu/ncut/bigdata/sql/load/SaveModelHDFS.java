package cn.edu.ncut.bigdata.sql.load;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * 手动指定数据源类型 HDFS
 * <p>
 * Created by Ocean lin on 2017/11/23.
 */
public class SaveModelHDFS {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("SaveModelHDFS"));
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().format("json").load("hdfs://spark01:9000/sql-load/people.json");
        df.save("hdfs://spark01:9000/sql-load/people_savemodel_java", "json", SaveMode.Append);

        sc.close();
    }
}
