package cn.edu.ncut.bigdata.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Ocean lin on 2017/10/18.
 */
public class ActionOperation {

    public static void main(String[] args) {
//        reduce();
//        count();
//        take();
//        saveAsTextFile();
        countByKey();
    }

    private static void saveAsTextFile() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("saveAsTextFile"));
        List<Integer> numbersList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbersList);
        JavaRDD<Integer> mapRDD = numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        // 直接将rdd中的数据，保存在HFDS文件中
        // 但是要注意，我们这里只能指定文件夹，也就是目录
        // 那么实际上，会保存为目录中的/double_number.txt/part-00000文件
        mapRDD.saveAsTextFile("hdfs://spark01:9000/number");
        sc.close();
    }


    private static void countByKey() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("countByKey"));
        List<Tuple2<String, String>> scoreList = Arrays.asList(
                new Tuple2<>("class1", "leo"),
                new Tuple2<>("class2", "jack"),
                new Tuple2<>("class1", "marry"),
                new Tuple2<>("class2", "tom"),
                new Tuple2<>("class2", "david"));
        JavaPairRDD<String, String> pairRDD = sc.parallelizePairs(scoreList);
        Map<String, Object> count = pairRDD.countByKey();
        for (Map.Entry<String, Object> m : count.entrySet()) {
            System.out.println(m.getKey() + " : " + m.getValue());
        }
    }


    private static void take() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("take"));

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numberList);
        List<Integer> integerList = numberRDD.take(3);
        for (Integer in : integerList)
            System.out.println(in);
    }

    private static void count() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("count"));

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numberList);
        long count = numberRDD.count();
        System.out.println(count);
    }

    /**
     * 求和操作
     */
    private static void reduce() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("reduce"));

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        Integer res = numbers.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(res);
        sc.close();
    }
}
