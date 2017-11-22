package cn.edu.ncut.bigdata.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by Ocean lin on 2017/11/22.
 */
public class DataFrameOperation {

    public static void main(String[] args) {
        SQLContext sqlContext =
                new SQLContext(new SparkContext(new SparkConf().setAppName("DataFrameOperation")));

        // 可以理解成一张表
        DataFrame dataFrame = sqlContext.read().json("hdfs://spark01:9000/students.json");
        // 打印dataFrame  select * from [TABLE]
        dataFrame.show();

        // 打印dataFrame的元数据（schema）
        dataFrame.printSchema();

        // 查询某列所有的数据
        dataFrame.select("name").show();

        // 查询某几列的数据，并对列进行计算
        dataFrame.select(dataFrame.col("name"), dataFrame.col("age").plus(1)).show();

        // 根据某一列的值进行过滤
        dataFrame.filter(dataFrame.col("age").gt(18)).show();

        // 分组聚合
        dataFrame.groupBy(dataFrame.col("age")).count().show();

    }

}
