package cn.edu.ncut.bigdata.sql.load;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ocean lin on 2017/11/24.
 */
public class JsonDataSource {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JsonDataSource"));
        SQLContext sqlContext = new SQLContext(sc);
        // 针对json文件，创建DataFrame（针对json文件创建DataFrame）
        DataFrame stuDF = sqlContext.read().json("hdfs://spark01:9000/spark-study/students.json");

        // （注册临时表，针对临时表执行sql语句）
        stuDF.registerTempTable("stus");
        DataFrame stu80DF = sqlContext.sql("select name,score from stus where score > 80");
        stu80DF.show();
        System.out.println("===============DF show================");

        // （将DataFrame转换为rdd，执行transformation操作）
        List<String> stuNameList = stu80DF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();


        // （针对包含json串的JavaRDD，创建DataFrame）
        List<String> stuList = new ArrayList<>();
        stuList.add("{\"name\":\"Leo\",\"age\":24}");
        stuList.add("{\"name\":\"Marry\",\"age\":23}");
        stuList.add("{\"name\":\"Jack\",\"age\":27}");
        JavaRDD<String> stuInfoRDD = sc.parallelize(stuList);
        DataFrame stuInfoDF = sqlContext.read().json(stuInfoRDD);

        stuInfoDF.registerTempTable("stuInfo");

        String where = "(";
        for (String s : stuNameList) {
            where += "'" + s + "',";
        }
        where = where.substring(0, where.length() - 1);
        where += ")";

        DataFrame stu80InfoDF = sqlContext.sql("select * from stuInfo where name in " + where);
        stu80InfoDF.show();

        // 然后将两份数据的DataFrame，转换为JavaPairRDD，执行join transformation
        // （将DataFrame转换为JavaRDD，再map为JavaPairRDD，然后进行join）
        JavaPairRDD<String, Tuple2<Integer, Integer>> javaPairRDD = stu80InfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(String.valueOf(row.get(1)), Integer.valueOf(row.get(0).toString()));
            }
        }).join(stu80DF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(String.valueOf(row.get(0)), Integer.valueOf(row.get(1).toString()));
            }
        }));

        JavaRDD<Row> stuRDD = javaPairRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1, t._2._1, t._2._2);
            }
        });


        // （将JavaRDD，转换为DataFrame）
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame dataFrame = sqlContext.createDataFrame(stuRDD, structType);

        // （将DataFrame中的数据保存到外部的json文件中去）
        dataFrame.write().format("json").save("hdfs://spark01:9000/GoodStudent");

        sc.close();
    }
}
