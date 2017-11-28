package cn.edu.ncut.bigdata.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * 基于滑动窗口的热点搜索词实时统计
 * Created by Ocean lin on 2017/11/28.
 */
public class WindowHotWord {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WindowHotWord").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(6));

        // 搜索日志的格式
        // leo hello
        // tom world
        JavaReceiverInputDStream<String> searchLogDStream = jsc.socketTextStream("spark01", 9999);
        JavaPairDStream<String, Integer> searchWordPairDStream = searchLogDStream.map(new Function<String, String>() {

            @Override
            public String call(String s) throws Exception {
                return s.split(" ")[1];
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // 针对(searchWord, 1)的tuple格式的DStream，执行reduceByKeyAndWindow，滑动窗口操作

        // 之前的searchWordPairDStream为止，其实，都是不会立即进行计算的
        // 而是只是放在那里
        // 然后，等待我们的滑动间隔到了以后，10秒钟到了，会将之前60秒的RDD，因为一个batch间隔是，5秒，所以之前
        // 60秒，就有12个RDD，给聚合起来，然后，统一执行redcueByKey操作
        // 所以这里的reduceByKeyAndWindow，是针对每个窗口执行计算的，而不是针对某个DStream中的RDD
        JavaPairDStream<String, Integer> windowDStream = searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
            // 第二个参数，是窗口长度，这里是60秒
            // 第三个参数，是滑动间隔，这里是10秒
            // 也就是说，每隔10秒钟，将最近60秒的数据，作为一个窗口，进行内部的RDD的聚合，然后统一对一个RDD进行后续
        }, Durations.seconds(60), Durations.seconds(10));

        // windowDStream


        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }

}
