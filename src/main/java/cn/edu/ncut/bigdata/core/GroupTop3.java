package cn.edu.ncut.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by Ocean lin on 2017/10/19.
 */
public class GroupTop3 {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext
                (new SparkConf().setMaster("local").setAppName("GroupTop3"));
        JavaRDD<String> score = sc.textFile("D://spark//score.txt");
        JavaPairRDD<String, Integer> pairRDD = score.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
            }
        });
        JavaPairRDD<String, Iterable<Integer>> groupRDD = pairRDD.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> top3RDD = groupRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> s) throws Exception {
                Iterator<Integer> iterator = s._2.iterator();
                Integer[] top = new Integer[3];
                while (iterator.hasNext()) {
                    Integer curVal = iterator.next();
                    for (int i = 0; i < 3; i++) {
                        if (top[i] == null) {
                            top[i] = curVal;
                            break;
                        } else if (curVal > top[i]) {
                            for (int j = 2; j > i; j--) {
                                top[j] = top[j - 1];
                            }
                            top[i] = curVal;
                            break;
                        }
                    }
                }
                return new Tuple2<>(s._1, (Iterable<Integer>) Arrays.asList(top));
            }
        });

        top3RDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class: " + t._1);
                Iterator<Integer> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next());
                }
                System.out.println("====================================");

            }
        });
        sc.close();
    }

}
