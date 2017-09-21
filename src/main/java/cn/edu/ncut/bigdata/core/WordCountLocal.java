package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by Ocean lin on 2017/9/21.
 */
public class WordCountLocal {

    public static void main(String[] args) {
        //编写Spark应用程序

        //第一步：创建SparkConf对象
        //使用setMaster()可以设置spark应用程序要连接的Spark集群的master节点的URL
        //setMaster()设置为local表示在本地运行
        SparkConf sparkConf = new SparkConf().
                setAppName("WordCountLocal").setMaster("local");


        //第二步：创建JavaSparkContext对象
        //
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //第三步：要针对输入源(HDFS、Hive、本地文件etc)，创建一个初始的RDD
        //输入源中的数据会打伞，分配到RDD的每个partition中，从而形成一个初始的分布式的数据集
        JavaRDD<String> lines = sparkContext.textFile("C://Users//lenovo//Desktop//spark.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        //需要将每个单词映射为(单词,1)的这种格式
        //因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的累加
        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2(word, 1);
            }
        });


        JavaPairRDD<String,Integer> wordCounts = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordcount) throws Exception {
                System.out.println(wordcount._1+":"+wordcount._2);
            }
        });

        sparkContext.close();

    }
}
