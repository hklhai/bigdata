package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Ocean lin on 2017/10/18.
 */
public class PersistRDD {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("PersistRDD"));
        // cache()或者persist()的使用，是有规则的
        // 必须在transformation或者textFile等创建了一个RDD之后，直接连续调用cache()或persist()才可以
        // 如果你先创建一个RDD，然后单独另起一行执行cache()或persist()方法，是没有用的
        // 而且，会报错，大量的文件会丢失

        JavaRDD<String> lineRDD = sc.textFile("D://bigspark.txt").cache();
        long begin = System.currentTimeMillis();
        long count = lineRDD.count();
        long end = System.currentTimeMillis();
        System.out.println("count:" + count + " time:" + (end - begin));
        //第二次
        begin = System.currentTimeMillis();
        count = lineRDD.count();
        end = System.currentTimeMillis();
        System.out.println("2 times count:" + count + " time:" + (end - begin));

        sc.close();
    }
}
