package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Created by Ocean lin on 2017/10/12.
 */
public class HDFSFile {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("HDFSFile").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("hdfs://spark01:9000/spark.txt");

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
