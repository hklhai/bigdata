package cn.edu.ncut.bigdata.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ocean lin on 2017/11/28.
 */
// TODO: 2017/11/28  测试有问题 
public class Top3HotProduct {

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("Top3HotProduct").setMaster("local[2]");
        final JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 日志格式
        // leo iphone mobile_phone
        JavaReceiverInputDStream<String> product = jsc.socketTextStream("localhost", 9999);

        // 转换成mobile_phone-iphone 1
        product.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = " ".split(s);
                return new Tuple2<>(split[2] + "-" + split[1], 1);
            }
        }).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            // 然后针对60秒内的每个种类的每个商品的点击次数
            // foreachRDD，在内部，使用Spark SQL执行top3热门商品的统计
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, Durations.seconds(60), Durations.seconds(10)).foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            // catagoryProductCountRDD每个窗口的统计RDD
            @Override
            public Void call(JavaPairRDD<String, Integer> catagoryProductCountRDD) throws Exception {
                // 将该RDD转换为JavaRDD<Row>
                JavaRDD<Row> rowJavaRDD = catagoryProductCountRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> tuple) throws Exception {
                        String category = tuple._1.split("-")[0];
                        String product = tuple._1.split(" ")[1];
                        Integer count = tuple._2;
                        return RowFactory.create(category, product, count);
                    }
                });

                // 执行DataFrame转换
                List<StructField> structFieldList = new ArrayList<>();
                structFieldList.add(DataTypes.createStructField("category", DataTypes.StringType, true));
                structFieldList.add(DataTypes.createStructField("product", DataTypes.StringType, true));
                structFieldList.add(DataTypes.createStructField("count", DataTypes.IntegerType, true));
                StructType st = DataTypes.createStructType(structFieldList);

                HiveContext hiveContext = new HiveContext(catagoryProductCountRDD.context());
                DataFrame dataFrame = hiveContext.createDataFrame(rowJavaRDD, st);
                dataFrame.registerTempTable("products");
                // 使用开窗函数
                DataFrame top3DF = hiveContext.sql("select category,product,count from (" +
                        "select category,product,count," +
                        "ROW_NUMBER() OVER (PARTITION BY category order by count desc ) rank from products) tmp" +
                        " where rank >=3");
                top3DF.show();
                return null;
            }
        });


        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
