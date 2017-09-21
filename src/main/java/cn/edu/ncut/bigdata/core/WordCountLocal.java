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
 * Java版wordcount程序
 * Created by Ocean lin on 2017/9/21.
 */
public class WordCountLocal {

    public static void main(String[] args) {
        //编写Spark应用程序

        //第一步：创建SparkConf对象，设置Spark应用的配置信息
        //使用setMaster()可以设置spark应用程序要连接的Spark集群的master节点的URL
        //setMaster()设置为local表示在本地运行
        SparkConf sparkConf = new SparkConf().
                setAppName("WordCountLocal").setMaster("local");


        //第二步：创建JavaSparkContext对象
        //在Spark中，SparkContext是Spark所有功能的一个入口，无论是用Java、Scala、Python编写
            //必须有一个SparkContext，它的主要作用包括初始化Spark应用程序所需的一些核心组件，包括
            //调度器(DAGSchedule、TaskScheduler)，还会去到Spark、Master节点上进行祖册，等等
        //SparkContext在Spark应用中是最重要的一个对象
        //但是在Spark中，编写不同类型的Spark应用，使用SparkContext是不同的。
            //如果使用Scala，使用的就是原生的SparkContext对象
            //如果使用过的是Java，那么就是JavaSparkContext对象
            //如果开发的是Spark SQL程序，那么就是SQLContext，Hive就是HiveContext
            // 如果是开发Spark Streaming程序，那么就是它独有的SparkContext
            // 以此类推
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //第三步：要针对输入源(HDFS、Hive、本地文件etc)，创建一个初始的RDD
        //输入源中的数据会打散，分配到RDD的每个partition中，从而形成一个初始的分布式的数据集
        //我们这里呢，因为是本地测试，所以就是针对本地文件
        //SparkContext中，用于根据本地文件类型的输入源创建RDD的方法，叫做textFile()方法
        //Java中，创建的普通RDD，都叫做JavaRDD
        //在这里,RDD中，有元素这种概念，如果是HDFS或者本地文件，创建的RDD中，每个元素就相当于文件里的一行
        JavaRDD<String> lines = sparkContext.textFile("C://Users//lenovo//Desktop//spark.txt");

        //第四步：对初始RDD进行Transformation操作，也就是一些计算操作
        //通常操作会通过创建function，并配合RDD的map、flatMap等算子来执行
        //function：如果比较简单，通常创建指定Function的匿名内部类
        //但是如果function比较复杂，则会单独创建一个类，作为实现这个function接口的类

        //先将每一行拆分成单个的单词
        //flatMapFunction有两个泛型参数，分别代表了输入和输出的类型
        //这里的数据是String，因为是一行一行的文本，输出其实也是String，因为是每一行的文本
        //flatMap算子的作用，其实就是讲RDD的一个元素，给拆分成一个多个元素
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        //接下来需要将每个单词映射为(单词,1)的这种格式
            //因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的累加
        //mapToPair，其实就是讲每个元素，映射为一个(v1,v2)这样的Tuple2类型的元素
            //这里的tuple2就是Scala中的类型，包含了两个值
        //mapToPair这个算子要求的是与PairFunction配合使用
            //第一个泛型参数代表了输入类型,第二个和第三个泛型参数，代表输出的Tuple2的第一个值和第二个值的类型
        // JavaPariRDD的两个泛型参数，分别代表了tuple元素的第一个值和第二个值的类型
        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2(word, 1);
            }
        });

        //下面需要以单词作为key，统计每个单词出现的次数
        //这里需要使用reduceByKey这个算子，对于每个key对应的value，都进行reduce操作
        //比如JavaPairRDD中有几个元素，分别是(hello,1) (hello,1) (hello,1) (world,1)
        //reduce操作，相当于把第一个值和第二个值进行计算，然后再将第二个值与第三个值进行计算
        //比如这里的hello,那么就相当于，首先1+1=2，然后在将2+1=3
        //最后返回的JavaPairRDD中的元素，也是tuple，但是第一个值就是每个key，第二个值就是key的value
        //reduce之后的结果就相当于每个单词出现的次数
        JavaPairRDD<String,Integer> wordCounts = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });


        //到这里为止，我们通过几个Spark算子操作，已经统计出了单词的次数
        //但是，之前我们使用的flapMap、mapToPair、reduceByKey这种操作，都叫做transformation操作
        //一个Spark应用中，光有transformation操作，是不行的，是不会执行的，必须要一种叫做action
        //最后，可以使用一种叫做action操作的，比如foreach，来触发程序的执行
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordcount) throws Exception {
                System.out.println(wordcount._1+":"+wordcount._2);
            }
        });

        sparkContext.close();

    }
}
