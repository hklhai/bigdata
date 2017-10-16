package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created by Ocean lin on 2017/10/16.
 */
public class LineCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lineRDD = sc.textFile("D://hello.txt");
        JavaPairRDD<String, Integer> javaPairRDD = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2(s, 1);
            }
        });

        JavaPairRDD<String, Integer> countRDD = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        countRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t2) throws Exception {
                System.out.println(t2._1 + "appears :"+t2._2+ " times");
            }
        });

        sc.close();
    }
}
