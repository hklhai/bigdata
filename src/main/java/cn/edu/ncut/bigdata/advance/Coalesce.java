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
 * 联合，合并
 */
public class Coalesce {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Coalesce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // coalesce算子，功能是将RDD的partition缩减，减少
        // 将一定量的数据，压缩到更少的partition中去

        // 建议的使用场景，配合filter算子使用
        // 使用filter算子过滤掉很多数据以后，比如30%的数据，出现了很多partition中的数据不均匀的情况
        // 此时建议使用coalesce算子，压缩rdd的partition数量
        // 从而让各个partition中的数据都更加的紧凑

        // 公司原先有6个部门，裁员以后呢，有的部门中的人员就没了
        // 不同的部分人员不均匀，此时呢，做一个部门整合的操作，将不同的部门的员工进行压缩
        List<String> staffList = Arrays.asList("张三", "李四", "王二", "麻子",
                "赵六", "王五", "李大个", "王大妞", "小明", "小倩");
        JavaRDD<String> staffRDD = sc.parallelize(staffList, 6); // 初始6个部门，6个分区
        JavaRDD<String> staffClassRDD = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> stringList = new ArrayList<>();

                while (iterator.hasNext()) {
                    String staff = iterator.next();
                    stringList.add(staff + "-" + (index + 1));
                }

                return stringList.iterator();
            }
        }, true);

        for (String staffInfo : staffClassRDD.collect()) {
            System.out.println(staffInfo);
        }

        JavaRDD<String> stringJavaRDD = staffClassRDD.coalesce(3).mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> stringList = new ArrayList<>();

                while (iterator.hasNext()) {
                    String staff = iterator.next();
                    stringList.add(staff + "##" + (index + 1));
                }

                return stringList.iterator();
            }
        }, true);


        System.out.println("=================================");
        for (String staffInfo : stringJavaRDD.collect()) {
            System.out.println(staffInfo);
        }

        sc.close();
    }
}
