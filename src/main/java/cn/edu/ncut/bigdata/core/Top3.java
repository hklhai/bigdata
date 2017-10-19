package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * Created by Ocean lin on 2017/10/19.
 */
public class Top3 {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext
                (new SparkConf().setMaster("local").setAppName("Top3"));
        JavaRDD<String> lines = sc.textFile("D://spark//top.txt");
        JavaPairRDD<Integer, String> pairRDD = lines.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<>(Integer.valueOf(s), s);
            }
        });
        JavaRDD<String> mapRDD = pairRDD.sortByKey(false).map(new Function<Tuple2<Integer, String>, String>() {
            @Override
            public String call(Tuple2<Integer, String> v1) throws Exception {
                return v1._2;
            }
        });

        List<String> top3 = mapRDD.take(3);
        for (String s : top3)
            System.out.println(s);
    }
}
