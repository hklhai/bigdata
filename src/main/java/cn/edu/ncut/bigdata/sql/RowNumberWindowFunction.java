package cn.edu.ncut.bigdata.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Ocean lin on 2017/11/26.
 */
public class RowNumberWindowFunction {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("RowNumberWindowFunction").setMaster("local"));
        HiveContext hiveContext = new HiveContext(sc.sc());

        hiveContext.sql("drop table if exists sales");
        hiveContext.sql("create table if not exists sales (product string,catagory string,revenue bigint)");
        hiveContext.sql("load data " +
                " local inpath '/root/sparkstudy/file/sales.txt' " +
                "into table sales");

        // 开始编写我们的统计逻辑，使用row_number()开窗函数
        DataFrame top3DF = hiveContext.sql("select product,catagory,revenue from ( " +
                "select product,catagory,revenue, " +
                "row_number() over (partition by catagory order by revenue desc) rank " +
                "from sales ) sr " +
                "where rank <= 3 ");

        hiveContext.sql("drop table if exists top3");
        top3DF.saveAsTable("top3");

        sc.close();
    }
}
