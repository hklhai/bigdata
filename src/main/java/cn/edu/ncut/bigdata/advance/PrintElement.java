package cn.edu.ncut.bigdata.advance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Ocean lin on 2017/12/14.
 */
public class PrintElement {

    public static void main(String[] args) {
//        JavaSparkContext sc = new JavaSparkContext(
//                new SparkConf().setAppName("PrintElement").set("spark.default.parallelism", "2").setMaster("local"));
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("PrintElement"));
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> parallelizeRDD = sc.parallelize(list);

        parallelizeRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();
    }
}
