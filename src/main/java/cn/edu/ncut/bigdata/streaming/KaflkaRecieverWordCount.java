package cn.edu.ncut.bigdata.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 基于Kafka receiver方式的实时wordcount程序
 * Created by Ocean lin on 2017/11/27.
 */
public class KaflkaRecieverWordCount {

    public static void main(String[] args) {
        final String zk = "spark01:2181,spark02:2181,spark03:2181";

        SparkConf conf = new SparkConf().setAppName("KaflkaRecieverWordCount").setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(6));

        Map<String, Integer> topicThreadMap = new HashMap<>();
        topicThreadMap.put("WordCount", 1);

        // 使用KafkaUtils.createStream()方法，创建针对Kafka的输入数据流
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jsc,
                zk,
                "DefaultConsumerGroup",
                topicThreadMap
        );

        JavaPairDStream<String, Integer> wordCOunt = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> tuple2) throws Exception {
                return Arrays.asList(tuple2._2.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        wordCOunt.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
