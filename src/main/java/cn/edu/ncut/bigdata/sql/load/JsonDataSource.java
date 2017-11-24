package cn.edu.ncut.bigdata.sql.load;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ocean lin on 2017/11/24.
 */
public class JsonDataSource {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JsonDataSource"));
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame stuDF = sqlContext.read().json("hdfs://spark01:9000/spark-study/students.json");

        stuDF.registerTempTable("stus");
        DataFrame stu80DF = sqlContext.sql("select name,score from stus where score > 80");
        stu80DF.show();
        System.out.println("===============DF show================");
        stu80DF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        List<String> stuList = new ArrayList<>();
        stuList.add("{\"name\":\"Leo\",\"age\":24}");
        stuList.add("{\"name\":\"Marry\",\"age\":23}");
        stuList.add("{\"name\":\"Jack\",\"age\":27}");
        JavaRDD<String> stuInfoRDD = sc.parallelize(stuList);
        DataFrame stuInfoDF = sqlContext.read().json(stuInfoRDD);

        stuInfoDF.registerTempTable("stuInfo");

        String where = "(";
        for (String s : stuList) {
            where += "'" + s + "',";
        }
        where.substring(0, where.length() - 2);
        where += ")";

        DataFrame dataFrame = sqlContext.sql("select * from stuInfo where name in " + where);
        dataFrame.show();

        sc.close();
    }
}
