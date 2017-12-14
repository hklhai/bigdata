package cn.edu.ncut.bigdata.advance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Ocean lin on 2017/12/14.
 */
public class MapPartitionIndex {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("MapPartitionIndex").setMaster("local"));
        // 模拟数据
        List<String> studentNames = Arrays.asList("Leo", "Peter", "Alex", "Wei");
        JavaRDD<String> studentNamesRDD = sc.parallelize(studentNames, 2);


        JavaRDD<String> studentClassRDD = studentNamesRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> studentList = new ArrayList<>();
                while (iterator.hasNext()) {
                    String student = iterator.next();
                    studentList.add(student + "_" + (index + 1));
                }
                return studentList.iterator();
            }
        }, true);

        for (String stu_class : studentClassRDD.collect()) {
            System.out.println(stu_class);
        }

        sc.close();
    }
}
