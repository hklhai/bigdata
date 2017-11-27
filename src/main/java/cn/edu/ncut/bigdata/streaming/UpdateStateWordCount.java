package cn.edu.ncut.bigdata.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 基于updateStateByKey算子实现缓存机制的实时wordcount程序
 * Created by Ocean lin on 2017/11/27.
 */
public class UpdateStateWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("UpdateStateWordCount").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(6));

        // 第一点，如果要使用updateStateByKey算子，就必须设置一个checkpoint目录，开启checkpoint机制
        // 这样的话才能把每个key对应的state除了在内存中有，那么是不是也要checkpoint一份
        // 因为你要长期保存一份key的state的话，那么spark streaming是要求必须用checkpoint的，以便于在
        // 内存数据丢失的时候，可以从checkpoint中恢复数据


        // 开启checkpoint机制，只要调用jsc的checkpoint()方法，设置一个hdfs目录即可
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


        // 之前的WordCount，是不是直接就是pairs.reduceByKey
        // 然后，就可以得到每个时间段的batch对应的RDD，计算出来的单词计数
        // 然后，可以打印出那个时间段的单词计数

        // 统计每个单词的全局的计数
        // 统计出来，从程序启动开始，到现在为止，一个单词出现的次数，那么就之前的方式就不好实现
        // 就必须基于redis这种缓存，或者是mysql这种db，来实现累加

        // 使用updateStateByKey，可以实现直接通过Spark维护一份每个单词的全局的统计次数
        JavaPairDStream<String, Integer> wordCount = words.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            // 这里的Optional，相当于Scala中的样例类，就是Option，可以这么理解
            // 它代表了一个值的存在状态，可能存在，也可能不存在

            // 这里两个参数
            // 实际上，对于每个单词，每次batch计算的时候，都会调用这个函数
            // 第一个参数，values，相当于是这个batch中，这个key的新的值，可能有多个吧
            // 比如说一个hello，可能有2个1，(hello, 1) (hello, 1)，那么传入的是(1,1)
            // 第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型是你自己指定的
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> states) throws Exception {
                // 首先定义一个全局的单词计数
                int newvalue = 0;

                // 其次，判断，state是否存在，如果不存在，说明是一个key第一次出现
                // 如果存在，说明这个key之前已经统计过全局的次数了
                if (states.isPresent()) {
                    newvalue = states.get();
                }

                for (Integer integer : values) {
                    newvalue += integer;
                }

                return Optional.of(newvalue);
            }
        });
        wordCount.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
