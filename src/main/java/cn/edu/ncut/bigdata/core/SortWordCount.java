package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by Ocean lin on 2017/10/18.
 */
public class SortWordCount {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("SortWordCount"));

        JavaRDD<String> lines = sc.textFile("D://spark.txt");
        JavaRDD<String> mapRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String, Integer> pairRDD = mapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 到这里为止，就得到了每个单词出现的次数
        // 但是，问题是，我们的新需求，是要按照每个单词出现次数的顺序，降序排序
        // wordCounts RDD内的元素是什么？应该是这种格式的吧：(hello, 3) (you, 2)
        // 我们需要将RDD转换成(3, hello) (2, you)的这种格式，才能根据单词出现次数进行排序


        //key - value 反转映射
        JavaPairRDD<Integer, String> revertRDD = reduceRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tu) throws Exception {
                return new Tuple2<>(tu._2, tu._1);
            }
        });

        JavaPairRDD<Integer, String> sortRDD = revertRDD.sortByKey(false);

        //反转
        JavaPairRDD<String, Integer> resRDD = sortRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });


        resRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + ": " + t._2);
            }
        });

        sc.close();
    }

}
