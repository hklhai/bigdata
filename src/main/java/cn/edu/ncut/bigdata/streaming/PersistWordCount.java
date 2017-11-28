package cn.edu.ncut.bigdata.streaming;

import cn.edu.ncut.bigdata.common.ConnectionPool;
import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 基于持久化机制的WordCount程序
 * Created by Ocean lin on 2017/11/27.
 */
public class PersistWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("PersistWordCount").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(6));

        jsc.checkpoint("hdfs://spark01:9000/wordcount_checkpoint");
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
        JavaPairDStream<String, Integer> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });


        JavaPairDStream<String, Integer> wordCount = words.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> states) throws Exception {
                int newvalue = 0;

                if (states.isPresent()) {
                    newvalue = states.get();
                }

                for (Integer integer : values) {
                    newvalue += integer;
                }

                return Optional.of(newvalue);
            }
        });


        // 每次得到当前所有单词的统计次数之后，将其写入mysql存储，进行持久化，以便于后续的J2EE应用程序
        // 进行显示
        wordCount.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> wordCountPairRDD) throws Exception {
                // 调用RDD的foreachPartition方法
                wordCountPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordCountIterator) throws Exception {

                        // 给每一个partition获取一个连接
                        Connection connection = ConnectionPool.getConnection();

                        // 遍历partition数据，使用一个连接插入数据库
                        Tuple2<String, Integer> wordCount = null;
                        while (wordCountIterator.hasNext()) {
                            wordCount = wordCountIterator.next();
                            String sql = "insert into wordcount (word,count) " +
                                    "values ('" + wordCount._1 + "','" + wordCount._2 + "')";

                            Statement statement = connection.createStatement();
                            statement.execute(sql);
                        }

                        ConnectionPool.returnConnection(connection);
                    }
                });


                return null;
            }
        });


        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
