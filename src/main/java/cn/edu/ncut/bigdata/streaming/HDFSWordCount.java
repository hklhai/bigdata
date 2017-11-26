package cn.edu.ncut.bigdata.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by Ocean lin on 2017/11/26.
 */
public class HDFSWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("HDFSWordCount").setMaster("local[2]");

        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(6));

        JavaDStream<String> lines = context.textFileStream("hdfs://spark01:9000/streaming");

        JavaPairDStream<String, Integer> wordCount = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });


        Thread.sleep(5000);
        wordCount.print();

        context.start();
        context.awaitTermination();
        context.close();
    }
}
