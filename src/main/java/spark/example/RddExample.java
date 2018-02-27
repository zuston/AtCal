package spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by zuston on 2018/2/27.
 */
public class RddExample {
    public static void main(String[] args) {
        String logFile = "./data/data.txt"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(s->s.contains("a")).count();

        long numBs = logData.filter(s->s.contains("a")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        JavaRDD<String> result = sc.parallelize(Arrays.asList("dog","hear","dog","cat"));

        JavaPairRDD<String,Integer> pairRDD = result.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        for(Tuple2<String,Integer> tuple : pairRDD.collect()) {
            System.out.println(tuple._1() + ":" + tuple._2());
        }
    }
}
