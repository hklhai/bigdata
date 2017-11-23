package cn.edu.ncut.bigdata.sql.load;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Created by Ocean lin on 2017/11/23.
 */
public class ParquetLoad {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("ParquetLoad"));
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().parquet("hdfs://spark01:9000/sql-load/users.parquet");
        df.registerTempTable("users");
        DataFrame usersDF = sqlContext.sql("select * from users");
        System.out.println("===============usersDF===================");
        usersDF.show();
        System.out.println("===============show===================");
        List<String> userNameList = usersDF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();
        for (String s : userNameList)
            System.out.println("Name: " + s);

        sc.close();
    }
}
