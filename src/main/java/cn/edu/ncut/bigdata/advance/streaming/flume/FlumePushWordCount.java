package cn.edu.ncut.bigdata.advance.streaming.flume;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 基于Flume push方式的实时wordcount程序
 * Created by Ocean lin on 2017/12/18.
 */
public class FlumePushWordCount {

    public static void main(String[] args) {

        String kafka = "spark02:9092,spark03:9092";
        SparkConf conf = new SparkConf().setAppName("FlumePushWordCount").setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(6));

        // 在指定机器的指定端口上监听数据
        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
                FlumeUtils.createStream(jsc, "192.168.89.129", 8888);


        JavaPairDStream<String, Integer> wordCountRDD = flumeStream.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {
            @Override
            public Iterable<String> call(SparkFlumeEvent event) throws Exception {
                // 重要的转换方式
                String line = new String(event.event().getBody().array());
                return Arrays.asList(line.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCountRDD.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
