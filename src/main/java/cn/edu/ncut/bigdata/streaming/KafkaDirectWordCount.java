package cn.edu.ncut.bigdata.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 基于Kafka Direct方式的实时wordcount程序
 * Created by Ocean lin on 2017/11/27.
 */
public class KafkaDirectWordCount {

    public static void main(String[] args) {
        //String kafka = "192.168.89.128:2181,192.168.89.129:2181,192.168.89.130:2181";
        String kafka = "spark01:9092,spark02:9092,spark03:9092";
        SparkConf conf = new SparkConf().setAppName("KaflkaRecieverWordCount").setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(6));
        // 设置参数，不基于zookeeper
        Map<String, String> kafkaParam = new HashMap<>();
        kafkaParam.put("metadata.broker.list", kafka);

        // 然后，要创建一个set，里面放入，你要读取的topic
        // 可以并行读取多个topic
        Set<String> topicSet = new HashSet<>();
        topicSet.add("DirectTopic");

        // 创建输入DStream
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParam,
                topicSet);

        // 执行wordcount
        lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
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
        }).print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
