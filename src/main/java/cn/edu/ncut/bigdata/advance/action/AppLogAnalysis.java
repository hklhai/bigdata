package cn.edu.ncut.bigdata.advance.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * 移动端app访问流量日志分析
 * <p>
 * Created by Ocean lin on 2017/12/15.
 */
public class AppLogAnalysis {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("AppLogAnalysis")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> accessLogRDD = sc.textFile("D:\\spark\\advance\\access.log");

        // 将RDD映射为key-value格式
        JavaPairRDD<String, AccessLogInfo> accessLogPairRDD = accessLogRDD.mapToPair(new PairFunction<String, String, AccessLogInfo>() {
            @Override
            public Tuple2<String, AccessLogInfo> call(String s) throws Exception {
                String[] split = s.split("\t");

                Long timestamp = Long.valueOf(split[0]);
                String deviceID = split[1];
                Long upTraffic = Long.valueOf(split[2]);
                Long downTraffic = Long.valueOf(split[3]);

                return new Tuple2<>(deviceID, new AccessLogInfo(timestamp, upTraffic, downTraffic));
            }
        });

        // 根据deviceID进行聚合操作
        // 计算出每个deviceID的总上行流量、总下行流量以及最早访问时间
        JavaPairRDD<String, AccessLogInfo> reduceRDD = accessLogPairRDD.reduceByKey(new Function2<AccessLogInfo, AccessLogInfo, AccessLogInfo>() {
            @Override
            public AccessLogInfo call(AccessLogInfo v1, AccessLogInfo v2) throws Exception {
                long timestamp = v1.getTimeStamp() > v2.getTimeStamp() ? v2.getTimeStamp() : v1.getTimeStamp();
                long upTraffic = v2.getUpTracffic() + v1.getUpTracffic();
                long downTraffic = v2.getDownTracffic() + v1.getDownTracffic();
                return new AccessLogInfo(timestamp,upTraffic,downTraffic);
            }
        });





        sc.close();
    }
}
