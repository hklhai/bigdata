package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Created by Ocean lin on 2017/10/12.
 */
public class LocalFile {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("LocalFile").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("D://spark//spark.txt");

        JavaRDD<Integer> lineRDD = lines.map(new Function<String, Integer>() {

            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        });
        Integer sum = lineRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("Sum words:" + sum);
        sc.close();
    }
}
