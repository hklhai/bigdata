package cn.edu.ncut.bigdata.sql.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Ocean lin on 2017/11/25.
 */
public class HiveDataSource {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HiveDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建HiveContext，注意，这里，它接收的是SparkContext作为参数，不是JavaSparkContext
        HiveContext hiveContext = new HiveContext(sc.sc());

        // 第一个功能，使用HiveContext的sql()方法，可以执行Hive中能够执行的HiveQL语句

        // 判断是否存在student_infos表，如果存在则删除
        hiveContext.sql("drop table if exists student_infos ");
        hiveContext.sql("create table student_infos (name STRING,age INT )");
        hiveContext.sql("load data " +
                "local inpath '/root/sparkstudy/file/student_infos.txt' " +
                "into table student_infos");

        hiveContext.sql("drop table if exists student_scores ");
        hiveContext.sql("create table student_scores (name STRING,score INT)");
        hiveContext.sql("load data " +
                "local inpath '/root/sparkstudy/file/student_scores.txt' " +
                "into table student_scores");

        // 第二个功能，执行sql还可以返回DataFrame，用于查询
        // 执行sql查询，关联两张表，查询成绩大于80分的学生
        DataFrame stu80DF = hiveContext.sql("select si.name,si.age,ss.score  " +
                "from student_infos si , student_scores ss" +
                " where si.name = ss.name and ss.score > 80 ");

        // 第三个功能，可以将DataFrame中的数据，理论上来说，DataFrame对应的RDD的元素，是Row即可
        // 将DataFrame中的数据保存到hive表中
        hiveContext.sql("drop table if exists good_stu");
        stu80DF.saveAsTable("good_stu");

        // 第四个功能，可以用table()方法，针对hive表，直接创建DataFrame
        // 然后针对good_student_infos表，直接创建DataFrame
        DataFrame good_stuDF = hiveContext.table("good_stu");

        Row[] rows = good_stuDF.collect();
        for (Row stu : rows) {
            System.out.println(stu);
        }
        sc.close();

    }

}
