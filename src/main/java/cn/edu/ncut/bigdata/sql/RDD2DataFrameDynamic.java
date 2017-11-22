package cn.edu.ncut.bigdata.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ocean lin on 2017/11/22.
 */
public class RDD2DataFrameDynamic {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("RDD2DataFrameDynamic").setMaster("local"));
        SQLContext sqlContext = new SQLContext(sc);

        // 1. 创建一个普通的RDD，并将其转换为一个RDD<Row>的格式
        // 往Row中加数据的时候，要注意，什么格式的数据，就用什么格式转换一下，再加进去
        JavaRDD<String> linesRDD = sc.textFile("D://spark//students.txt", 1);
        JavaRDD<Row> rowJavaRDD = linesRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] strings = s.split(",");

                return RowFactory.create(Integer.valueOf(strings[0]), strings[1], Integer.valueOf(strings[2]));
            }
        });

        // 2. 动态构造元数据
        List<StructField> fields = new ArrayList<>();
        // 比如说，id、name等，field的名称和类型，可能都是在程序运行过程中，动态从mysql db里
        // 或者是配置文件中，加载出来的，是不固定的
        // 所以特别适合用这种编程的方式，来构造元数据
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(fields);


        // 3. 使用动态构造的元数据，将RDD转换为DataFrame
        DataFrame studentDf = sqlContext.createDataFrame(rowJavaRDD, structType);

        studentDf.registerTempTable("students");

        DataFrame teenDF = sqlContext.sql("select * from students where age <= 18");
        teenDF.show();
        System.out.println("============DataFrame end===================");


        JavaRDD<Row> rdd = teenDF.javaRDD();
        for (Row r : rdd.collect()) {
            System.out.println(r);
        }
        System.out.println("============RDD end===================");
    }
}
