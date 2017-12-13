package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Ocean lin on 2017/9/21.
 */
public class WordCountClusterForLog {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("WordCountClusterForLog");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //注意修改hdfs文件地址，协议为hdfs而不是http
        JavaRDD<String> lines = sparkContext.textFile("hdfs://192.168.89.129:9000/hello.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                Thread.sleep(20*1000);
                System.out.println("===========stdout flatMap=============");
                System.err.println("===========stderr flatMap=============");
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2(word, 1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                Thread.sleep(20*1000);
                System.out.println("===========stdout reduceByKey=============");
                System.err.println("===========stderr reduceByKey=============");
                return v1 + v2;
            }
        });
        List<Tuple2<String, Integer>> wordCountList = wordCounts.collect();
        for(Tuple2<String, Integer> wordCount : wordCountList)
        {
            System.out.println(wordCount);
        }
//        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> wordcount) throws Exception {
//                System.out.println(wordcount._1 + ":" + wordcount._2);
//            }
//        });

        sparkContext.close();

    }
}
