package cn.edu.ncut.bigdata.sql.jdbc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Ocean lin on 2017/11/25.
 */
public class JDBCDataSource {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JDBCDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建HiveContext，注意，这里，它接收的是SparkContext作为参数，不是JavaSparkContext
        SQLContext sqlContext = new SQLContext(sc.sc());


        // jdbc数据源
        // 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
        // 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
        // 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db / cache中

        Map<String, String> map = new HashMap<>();
        map.put("url", "jdbc:mysql://spark01:3306/hivedb");
        map.put("dbtable", "student_infos");
        DataFrame stuDF = sqlContext.read().format("jdbc").options(map).load();

        map.put("dbtable", "student_scores");
        DataFrame stuScoreDF = sqlContext.read().format("jdbc").options(map).load();


        //然后可以将DataFrame转换为JavaRDD
        JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = stuDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
            }
        }).join(stuScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
            }
        }));

        JavaRDD<Row> goodStuRDD = joinRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {

            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple2) throws Exception {
                return RowFactory.create(tuple2._1, tuple2._2._1, tuple2._2._2);
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if (row.getInt(2) > 80)
                    return true;
                else
                    return false;
            }
        });

        // 转换为DataFrame
        List<StructField> structFieldList = new ArrayList<>();
        ;
        structFieldList.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFieldList.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFieldList);
        DataFrame goodStuDF = sqlContext.createDataFrame(goodStuRDD, structType);

        // 输出DataFrame数据
        for (Row row : goodStuDF.collect())
            System.out.println(row);


        // 将DataFrame中的数据保存到mysql表中
        // 这种方式是在企业里很常用的，有可能是插入mysql、有可能是插入hbase，还有可能是插入redis缓存
        goodStuDF.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {

                String sql = "insert into good_students (name,age,score) values " +
                        "('" + String.valueOf(row.getString(0)) + "',"
                        + Integer.valueOf(row.getInt(1)) + "," +
                        +Integer.valueOf(row.getInt(2)) + ")";
                Class.forName("com.mysql.jdbc.Driver");
                Connection connection = null;
                Statement statement = null;

                try {
                    connection = DriverManager.
                            getConnection("jdbc:mysql://spark01:3306/hivedb", "", "");
                    statement = connection.createStatement();
                    statement.execute(sql);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (connection != null)
                        connection.close();
                    if (statement != null)
                        statement.close();
                }
            }
        });

        sc.close();
    }
}
