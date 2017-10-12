package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Ocean lin on 2017/10/12.
 */
public class ParallelizeCollection {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        // 要通过并行化集合的方式创建RDD，那么就调用SparkContext以及其子类，的parallelize()方法
        List<Integer> numList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numRDD = javaSparkContext.parallelize(numList);

        // 执行reduce算子操作
        // 相当于，先进行1 + 2 = 3；然后再用3 + 3 = 6；然后再用6 + 4 = 10。。。以此类推
        int sum = numRDD.reduce(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        javaSparkContext.close();
        System.out.println("The Sum is :" + sum);

    }
}
