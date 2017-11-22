package cn.edu.ncut.bigdata.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 使用反射的方式将RDD转化为DataFrame
 * <p>
 * Created by Ocean lin on 2017/11/22.
 */
public class RDD2DataFrameReflect {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("RDD2DataFrameReflect").setMaster("local"));
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> linesRDD = sc.textFile("D://spark//students.txt", 1);
        JavaRDD<Student> studentJavaRDD = linesRDD.map(new Function<String, Student>() {
            @Override
            public Student call(String s) throws Exception {
                String[] strings = s.split(",");
                Student student = new Student(Integer.valueOf(strings[0].trim()),
                        strings[1], Integer.valueOf(strings[2].trim()));
                return student;
            }
        });

        // 使用反射的方式，将RDD转换为DataFrame
        // 将Student.class传入进去，其实就是用反射的方法来创建DataFrame
        // 因为Student.class本身就是反射的一个应用
        // 然后底层还得通过对Student Class进行反射，来获取其中的field
        // 这里要求JavaBean必须实现Serializable接口，是可序列化的
        DataFrame dataFrame = sqlContext.createDataFrame(studentJavaRDD, Student.class);

        // 拿到一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据进行SQL查询
        dataFrame.registerTempTable("students");

        // 针对students临时表执行SQL语句，查询年龄<18
        DataFrame teenager = sqlContext.sql("select * from students where age <= 18");
        teenager.show();
        System.out.println("======================dataframe end===================");

        // DataFrame -> RDD
        JavaRDD<Row> rowJavaRDD = teenager.javaRDD();
        JavaRDD<Student> teenagerJavaRDD = rowJavaRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                Student student = new Student(row.getInt(0), row.getString(2),
                        row.getInt(1));
                return student;
            }
        });
        teenagerJavaRDD.foreach(new VoidFunction<Student>() {
            @Override
            public void call(Student student) throws Exception {
                System.out.println(student.getId() + ":" + student.getName() + ":" + student.getAge());
            }
        });

        System.out.println("======================dataframe end===================");


    }


}
