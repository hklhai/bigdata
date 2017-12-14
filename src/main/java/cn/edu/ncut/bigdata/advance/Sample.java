package cn.edu.ncut.bigdata.advance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Ocean lin on 2017/12/14.
 */
public class Sample {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("Sample").setMaster("local"));
        // 模拟数据
        List<String> staffNames = Arrays.asList("Leo", "Peter", "Alex", "Wei",
                "Lee", "Ann", "Bob", "LiLei", "Han", "Ken");
        JavaRDD<String> staffNamesRDD = sc.parallelize(staffNames, 2);

        // sample算子
        // 可以使用指定的比例，比如说0.1或者0.9，从RDD中随机抽取10%或者90%的数据
        // 从RDD中随机抽取数据的功能
        // 推荐不要设置第三个参数，feed
        JavaRDD<String> luckey = staffNamesRDD.sample(false, 0.2);
        for (String name : luckey.collect()) {
            System.out.println(name);
        }
        sc.close();
    }
}
