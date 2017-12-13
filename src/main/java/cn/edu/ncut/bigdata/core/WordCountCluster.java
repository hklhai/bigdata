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
import java.util.List;

/**
 * Created by Ocean lin on 2017/9/21.
 */
public class WordCountCluster {

    public static void main(String[] args) {

        // 如果要在spark集群上运行，需要修改的，只有两个地方
        // 第一，将SparkConf的setMaster()方法给删掉，默认它自己会去连接
        // 第二，我们针对的不是本地文件了，修改为hadoop hdfs上的真正的存储大数据的文件

        // 实际执行步骤：
        // 1、将spark.txt文件上传到hdfs上去   hadoop fs -put spark.txt /spark.txt
        // 2、使用我们最早在pom.xml里配置的maven插件，对spark工程进行打包
        // 3、将打包后的spark工程jar包，上传到机器上执行
        // 4、编写spark-submit脚本
        // 5、执行spark-submit脚本，提交spark应用到集群执行

        SparkConf sparkConf = new SparkConf()
                .setAppName("WordCountCluster")
                .setMaster("spark://192.168.89.129:7077");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //注意修改hdfs文件地址，协议为hdfs而不是http
        JavaRDD<String> lines = sparkContext.textFile("hdfs://192.168.89.129:9000/hello.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2(word, 1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        List<Tuple2<String, Integer>> wordCountList = wordCounts.collect();
        for(Tuple2<String, Integer> wordCount : wordCountList)
        {
            System.out.println(wordCount);
        }
//        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> wordcount) throws Exception {
//                System.out.println(wordcount._1 + ":" + wordcount._2);
//            }
//        });

        sparkContext.close();

    }
}
