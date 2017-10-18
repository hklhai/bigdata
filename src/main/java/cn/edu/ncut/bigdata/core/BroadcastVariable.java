package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Ocean lin on 2017/10/18.
 */
public class BroadcastVariable {

    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("BroadcastVariable"));
        List<Integer> numbersList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbersList);

        // 在java中，创建共享变量，就是调用SparkContext的broadcast()方法
        // 获取的返回结果是Broadcast<T>类型
        final int factor = 3;
        final Broadcast<Integer> broadcast = sc.broadcast(factor);

        JavaRDD<Integer> map = numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * broadcast.value();
            }
        });

        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }
}
