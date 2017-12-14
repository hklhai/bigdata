package cn.edu.ncut.bigdata.advance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Ocean lin on 2017/12/14.
 */
public class PlusClosureVariable {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("PlusClosureVariable"));
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> parallelizeRDD = sc.parallelize(list);

        final List<Integer> closureNum = new ArrayList<>();
        closureNum.add(0);

        parallelizeRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                Integer sum = closureNum.get(0);
                sum += integer;
                closureNum.set(0, sum);
//                System.out.println("+++++++++++++++++++++++++++");
//                System.out.println(sum);
            }
        });
        // 闭包在local和standalone中均不起作用
        System.out.println("=============================");
        System.out.println(closureNum.get(0));

        sc.close();
    }
}
