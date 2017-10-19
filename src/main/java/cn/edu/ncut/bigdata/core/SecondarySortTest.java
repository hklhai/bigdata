package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created by Ocean lin on 2017/10/18.
 */
public class SecondarySortTest {

    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("SecondarySortKey").setMaster("local"));
        JavaRDD<String> numberRDD = sc.textFile("D://spark//sort.txt");

        JavaPairRDD<SecondarySortKey, String> pairRDD = numberRDD.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<>(new SecondarySortKey(Integer.valueOf(split[0]), Integer.valueOf(split[1])), s);
            }
        });

        JavaPairRDD<SecondarySortKey, String> sortRDD = pairRDD.sortByKey();
        JavaRDD<String> res = sortRDD.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });

        res.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
