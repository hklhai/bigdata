package cn.edu.ncut.bigdata.transfomation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Ocean lin on 2017/10/16.
 */
public class TransformationOperation {

    public static void main(String[] args) {

//        map();

//        filter();

//        flatMap();

//        reduceByKey();

//        groupByKey();

//        sortByKey();

//        join();

        cogroup();
    }

    private static void cogroup() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("cogroup");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<>(1, "leo"),
                new Tuple2<>(2, "jack"),
                new Tuple2<>(3, "tom"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 90),
                new Tuple2<>(3, 60),
                new Tuple2<>(1, 70),
                new Tuple2<>(2, 80),
                new Tuple2<>(3, 50));
        JavaPairRDD<Integer, String> studentRDD = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoreList);
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = studentRDD.cogroup(scoreRDD);
        cogroup.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println(t._1 + " " + t._2._1 + " " + t._2._2);
                System.out.println("===========================");
            }
        });
        sc.close();
    }

    private static void join() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("join");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<>(1, "hk"),
                new Tuple2<>(2, "lee"),
                new Tuple2<>(3, "alex"),
                new Tuple2<>(4, "tom")
        );
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 90),
                new Tuple2<>(2, 79),
                new Tuple2<>(3, 88),
                new Tuple2<>(4, 74)
        );

        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoreList);
        JavaPairRDD<Integer, String> studentRDD = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = studentRDD.join(scoreRDD);
        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println("id:" + t._1 + " name:" + t._2._1 + " score:" + t._2._2);
            }
        });

        sc.close();
    }

    private static void sortByKey() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sortByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<>(65, "lee"),
                new Tuple2<>(70, "alex"),
                new Tuple2<>(97, "hk"),
                new Tuple2<>(87, "mary")
        );
        JavaPairRDD<Integer, String> javaPairRDD = sc.parallelizePairs(list);
        JavaPairRDD<Integer, String> sortRDD = javaPairRDD.sortByKey(false);
        sortRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println("sorce:" + t._1 + " " + "name:" + t._2);
            }
        });
        sc.close();
    }

    private static void flatMap() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("flatMap");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("hello you", "hello me", "hello world");
        JavaRDD<String> stringJavaRDD = sc.parallelize(list);
        JavaRDD<String> wordRDD = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        wordRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("word:" + s);
            }
        });

        sc.close();
    }


    private static void map() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("map");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> numbersRDD = sparkContext.parallelize(numbers);

        JavaRDD<Integer> javaRDD = numbersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        javaRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer o) throws Exception {
                System.out.println("value is :" + o.toString());
            }
        });
        sparkContext.close();
    }


    private static void filter() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("filter");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        // filter算子，传入的也是Function，其他的使用注意点，实际上和map是一样的
        // 但是，唯一的不同，就是call()方法的返回类型是Boolean
        // 每一个初始RDD中的元素，都会传入call()方法，此时你可以执行各种自定义的计算逻辑
        // 来判断这个元素是否是你想要的
        // 如果你想在新的RDD中保留这个元素，那么就返回true；否则，不想保留这个元素，返回false
        JavaRDD<Integer> evenRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });
        evenRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("even number : : " + integer);
            }
        });
        sc.close();
    }

    private static void groupByKey() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("groupByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> classData = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 75),
                new Tuple2<>("class1", 90),
                new Tuple2<>("class2", 65)
        );
        JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(classData);
        // groupByKey算子，返回的还是JavaPairRDD
        // 但是，JavaPairRDD的第一个泛型类型不变，第二个泛型类型变成Iterable这种集合类型
        // 也就是说，按照了key进行分组，那么每个key可能都会有多个value，此时多个value聚合成了Iterable
        // 那么接下来，我们是不是就可以通过groupedScores这种JavaPairRDD，很方便地处理某个分组内的数据
        JavaPairRDD<String, Iterable<Integer>> groupRDD = scoresRDD.groupByKey();
        groupRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class:" + t._1);
                Iterator<Integer> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next());
                }
                System.out.println("================================================");
            }
        });

    }


    private static void reduceByKey() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("reduceByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> classData = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 75),
                new Tuple2<>("class1", 90),
                new Tuple2<>("class2", 65)
        );
        JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(classData);
        JavaPairRDD<String, Integer> javaPairRDD = scoresRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        javaPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + ":" + t._2);
            }
        });

        sc.close();
    }


}
