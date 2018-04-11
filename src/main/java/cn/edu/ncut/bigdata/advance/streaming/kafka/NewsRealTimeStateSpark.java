package cn.edu.ncut.bigdata.advance.streaming.kafka;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 新闻网站关键指标实时统计
 * <p>
 * Created by Ocean lin on 2017/12/18.
 *
 * @author Lin
 */
public class NewsRealTimeStateSpark {
    public static void main(String[] args) throws Exception {
        String kafka = "192.168.89.129:9092,192.168.89.130:9092";
        SparkConf conf = new SparkConf().setAppName("NewsRealTimeStateSpark").setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
        // 设置参数，不基于zookeeper
        Map<String, String> kafkaParam = new HashMap<>(5);
        kafkaParam.put("metadata.broker.list", kafka);

        // 然后，要创建一个set，里面放入，你要读取的topic
        // 可以并行读取多个topic
        Set<String> topicSet = new HashSet<>();
        topicSet.add("news-access");

        // 创建输入DStream
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParam,
                topicSet);

        // 过滤出访问日志
        JavaPairDStream<String, String> viewRDD = lines.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String[] split = v1._2.split(" ");
                if ("view".equals(split[5])) {
                    return true;
                }
                return false;

            }
        });

        // 统计第一个指标：每10秒内的各个页面的pv
        // pv(viewRDD);

        // 统计第二个指标：每10秒内的各个页面的uv
        // uv(lines);


        // 统计第三个指标：实时注册用户数
        // realTimeRegisterUserNum(lines);


        // 统计第四个指标：实时用户跳出数
        calculateUserJumpCount(viewRDD);

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

    private static void realTimeRegisterUserNum(JavaPairInputDStream<String, String> lines) {
        JavaPairDStream<String, String> registerDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String[] split = v1._2.split(" ");
                if ("register".equals(split[5])) {
                    return true;
                } else {
                    return false;
                }
            }
        });
        registerDStream.count().print();
    }

    private static void uv(JavaPairInputDStream<String, String> lines) {
        JavaDStream<String> pageIdUserIdDStream = lines.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                String[] split = v1._2.split(" ");
                String pageId = split[3].equalsIgnoreCase("null") ? "-1" : split[3];
                return split[2] + "_" + pageId;
            }
        });

        JavaDStream<String> distinctRStream = pageIdUserIdDStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
                return v1.distinct();
            }
        });

        JavaPairDStream<Long, Long> pageIdDStrean = distinctRStream.mapToPair(new PairFunction<String, Long, Long>() {

            @Override
            public Tuple2<Long, Long> call(String s) throws Exception {
                String[] split = s.split("_");
                return new Tuple2<>(Long.valueOf(split[1]), 1L);
            }
        });

        JavaPairDStream<Long, Long> uv = pageIdDStrean.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        uv.print();
    }

    private static void pv(JavaPairDStream<String, String> viewRDD) {
        JavaPairDStream<Long, Long> pageIdDStream = viewRDD.mapToPair(new PairFunction<Tuple2<String, String>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, String> tuple2) throws Exception {
                String[] split = tuple2._2.split(" ");

                return new Tuple2<>(Long.valueOf(split[3]), 1L);
            }
        });

        JavaPairDStream<Long, Long> pagePvDStream = pageIdDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        pagePvDStream.print();
        // 在计算出每10秒钟的页面pv之后，企业级项目中，应该持久化到mysql，或redis中，对每个页面的pv进行累加
        // JAVAEE系统，就可以从mysql或redis中，读取page pv实时变化的数据，以及曲线图
    }

    /**
     * 计算用户跳出数量
     *
     * @param accessDStream
     */
    private static void calculateUserJumpCount(JavaPairDStream<String, String> accessDStream) {
        JavaPairDStream<Long, Long> useridDStream = accessDStream.mapToPair(

                new PairFunction<Tuple2<String, String>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, String> tuple)
                            throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");
                        Long userid = Long.valueOf("null".equalsIgnoreCase(logSplited[2]) ? "-1" : logSplited[2]);
                        return new Tuple2<Long, Long>(userid, 1L);
                    }

                });

        JavaPairDStream<Long, Long> useridCountDStream = useridDStream.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        JavaPairDStream<Long, Long> jumpUserDStream = useridCountDStream.filter(

                new Function<Tuple2<Long, Long>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<Long, Long> tuple) throws Exception {
                        if (tuple._2 == 1) {
                            return true;
                        } else {
                            return false;
                        }
                    }

                });

        JavaDStream<Long> jumpUserCountDStream = jumpUserDStream.count();
        jumpUserCountDStream.print();
    }
}
