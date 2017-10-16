package cn.edu.ncut.bigdata.transfomation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Ocean lin on 2017/10/16.
 */
public class TransformationOperation {

    public static void main(String[] args) {

//        map();

        filter();
    }

    private static void map() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TransformationOperationMap");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> numbersRDD = sparkContext.parallelize(numbers);

        JavaRDD<Object> javaRDD = numbersRDD.map(new Function<Integer, Object>() {
            @Override
            public Object call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        javaRDD.foreach(new VoidFunction<Object>() {
            @Override
            public void call(Object o) throws Exception {
                System.out.println("value is :" + o.toString());
            }
        });
    }


    private static void filter() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TransformationOperationFilter");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        JavaRDD<Integer> evenRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });
        evenRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("even number : : " + integer);
            }
        });
    }


}
