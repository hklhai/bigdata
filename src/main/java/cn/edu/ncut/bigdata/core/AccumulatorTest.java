package cn.edu.ncut.bigdata.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Ocean lin on 2017/10/18.
 */
public class AccumulatorTest {

    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("AccumulatorTest"));
        List<Integer> numbersList = Arrays.asList(1, 2, 3, 4, 5);
        final Accumulator<Integer> accumulator = sc.accumulator(0);
        JavaRDD<Integer> javaRDD = sc.parallelize(numbersList);
        javaRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                accumulator.add(integer);
            }
        });
        System.out.println(accumulator);

        sc.close();
    }
}
