package cn.edu.ncut.bigdata.advance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.List;

/**
 * uv统计案例
 * Created by Ocean lin on 2017/12/14.
 */
public class Distinct {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Distinct")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // distinct算子
        // 对rdd中的数据进行去重

        // uv统计案例
        // uv：user view，每天每个用户可能对网站会点击多次
        // 此时，需要对用户进行去重，然后统计出每天有多少个用户访问了网站
        // 而不是所有用户访问了网站多少次（pv）
        List<String> accessLogs = Arrays.asList(
                "user1 2016-01-01 23:58:42",
                "user1 2016-01-01 23:58:43",
                "user1 2016-01-01 23:58:44",
                "user2 2016-01-01 12:58:42",
                "user2 2016-01-01 12:58:46",
                "user3 2016-01-01 12:58:42",
                "user4 2016-01-01 12:58:42",
                "user5 2016-01-01 12:58:42",
                "user6 2016-01-01 12:58:42",
                "user6 2016-01-01 12:58:45");
        JavaRDD<String> accessLogsRDD = sc.parallelize(accessLogs);
        JavaRDD<String> javaRDD = accessLogsRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split[0]);
            }
        });
        for (String s : javaRDD.distinct().collect()) {
            System.out.println(s);
        }
        System.out.println("uv:" + javaRDD.distinct().collect().size());

        sc.close();
    }
}
