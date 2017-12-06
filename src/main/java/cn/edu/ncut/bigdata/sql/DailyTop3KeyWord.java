package cn.edu.ncut.bigdata.sql;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 每日热点搜索词Top 3
 * Created by Ocean lin on 2017/12/6.
 */
public class DailyTop3KeyWord {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("DailyTop3KeyWord"));
        // HiveContext需要使用SparkContext，从JavaSparkContext中获取SparkContext
        HiveContext hiveContext = new HiveContext(sc.sc());

        // 查询条件
        Map<String, List<String>> queryParam = new HashMap<>();
        queryParam.put("city", Arrays.asList("beijing"));
        queryParam.put("platform", Arrays.asList("android"));
        queryParam.put("version", Arrays.asList("1.0", "1.2", "1.5", "2.0"));

        // 优化思路，将queryParam封装为Broadcast广播变量，每个worker节点仅一份数据
        final Broadcast<Map<String, List<String>>> broadcast = sc.broadcast(queryParam);


        // load
        JavaRDD<String> keywordRDD = sc.textFile("hdfs://spark01:9000/spark-study/keyword.txt");

        // filter查询条件
        JavaRDD<String> filterRDD = keywordRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String log) throws Exception {
                String[] line = log.split(" ");
                // 日期 用户 搜索词 城市 平台 版本
                String city = line[3];
                String platform = line[4];
                String version = line[5];
                Map<String, List<String>> queryListMap = broadcast.value();

                if (!queryListMap.get("city").contains(city))
                    return false;
                if (!queryListMap.get("platform").contains(platform))
                    return false;
                if (!queryListMap.get("version").contains(version))
                    return false;

                return true;
            }
        });


        // 转换成（日期_搜索词 用户）格式
        JavaPairRDD<String, String> date_word_user_RDD = filterRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] line = s.split(" ");
                // 日期 用户 搜索词 城市 平台 版本
                return new Tuple2<>(line[0] + "_" + line[2], line[1]);
            }
        });

        JavaPairRDD<String, Iterable<String>> date_word_RDD = date_word_user_RDD.groupByKey();

        // 对date_word按照用户去除重复 每天每个搜索词的UV
        JavaPairRDD<String, Long> uvRDD = date_word_RDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> t) throws Exception {
                String word = t._1;
                Iterator<String> userIterator = t._2.iterator();
                List userList = IteratorUtils.toList(userIterator);
                Set<String> uniqueUser = new HashSet<>();
                uniqueUser.addAll(userList);
                return new Tuple2<>(word, Long.valueOf(uniqueUser.size()));
            }
        });

        // uvRDD转换成DataFrame
        // 先转成RDD<Row>
        JavaRDD<Row> uvRowRDD = uvRDD.map(new Function<Tuple2<String, Long>, Row>() {
            @Override
            public Row call(Tuple2<String, Long> t) throws Exception {
                String[] date_word = t._1.split("_");
                return RowFactory.create(date_word[0], date_word[1], t._2);
            }
        });

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("word", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("uv", DataTypes.LongType, true));

        StructType structType = DataTypes.createStructType(structFields);


        DataFrame dataFrame = hiveContext.createDataFrame(uvRowRDD, structType);
        dataFrame.registerTempTable("uv_logs");
        DataFrame top3DF = hiveContext.sql("select date,word,uv " +
                " from (select date,word,uv,ROW_NUMBER() OVER (partition by date order by uv desc ) rank from uv_logs ) " +
                " tmp where rank<=3 ");

        // 将top3DF-> RDD
        // 计算top3搜索词的总数
        top3DF.javaRDD();


        sc.close();
    }
}
