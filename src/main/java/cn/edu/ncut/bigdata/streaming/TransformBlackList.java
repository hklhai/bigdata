package cn.edu.ncut.bigdata.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ocean lin on 2017/11/27.
 */
public class TransformBlackList {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TransformBlackList").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(6));

        // 用户对我们的网站上的广告可以进行点击
        // 点击之后，是不是要进行实时计费，点一下，算一次钱
        // 但是，对于那些帮助某些无良商家刷广告的人，那么我们有一个黑名单
        // 只要是黑名单中的用户点击的广告，我们就给过滤掉
        List<Tuple2<String, Boolean>> blacklist = new ArrayList<>();
        blacklist.add(new Tuple2<>("tom", true));
        final JavaPairRDD<String, Boolean> balckListRDD = jsc.sc().parallelize(blacklist).mapToPair(new PairFunction<Tuple2<String, Boolean>, String, Boolean>() {
            @Override
            public Tuple2<String, Boolean> call(Tuple2<String, Boolean> stringBooleanTuple2) throws Exception {
                return stringBooleanTuple2;
            }
        });

        // 这里的日志格式，就简化一下，就是date username的方式(2017,tom)
        JavaReceiverInputDStream<String> adsClickLogDStream = jsc.socketTextStream("spark01", 9999);

        // 所以，要先对输入的数据，进行一下转换操作，变成，(username, date username)
        // 以便于，后面对每个batch RDD，与定义好的黑名单RDD进行join操作
        JavaPairDStream<String, String> userLogClickDStream = adsClickLogDStream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[1], s);
            }
        });


        // 然后，就可以执行transform操作了，将每个batch的RDD，与黑名单RDD进行join、filter、map等操作
        // 实时进行黑名单过滤
        JavaDStream<String> validateRDD = userLogClickDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userLogClickRDD) throws Exception {
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = userLogClickRDD.leftOuterJoin(balckListRDD).filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    // 这里的tuple，就是每个用户，对应的访问日志，和在黑名单中的状态
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        if (tuple._2._2.isPresent() && tuple._2._2.get())
                            return false;
                        else
                            return true;
                    }
                });
                // 此时，filteredRDD中，就只剩下没有被黑名单过滤的用户点击了
                // 进行map操作，转换成我们想要的格式
                JavaRDD<String> mapRDD = filterRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        // 用户、点击日志
                        return tuple._2._1;
                    }
                });
                return mapRDD;
            }
        });

        // 打印有效的广告点击日志
        // 其实在真实企业场景中，这里后面就可以走写入kafka、ActiveMQ等这种中间件消息队列
        // 然后再开发一个专门的后台服务，作为广告计费服务，执行实时的广告计费，这里就是只拿到了有效的广告点击
        validateRDD.foreach(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> stringJavaRDD) throws Exception {
                System.out.println(stringJavaRDD.collect());
                return null;
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
