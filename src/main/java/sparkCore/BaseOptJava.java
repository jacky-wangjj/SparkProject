package sparkCore;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @ClassName BaseOptJava
 * @Description TODO
 * @Author wangjj
 * @Date 2019/5/13 16:04
 */
public class BaseOptJava {
    public static void parallelize(JavaSparkContext sc) {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> distData = sc.parallelize(data);
    }

    static class GetLength implements Function<String, Integer> {
        @Override
        public Integer call(String s) {
            return s.length();
        }
    }

    static class Sum implements Function2<Integer, Integer, Integer> {
        @Override
        public Integer call(Integer integer, Integer integer2) throws Exception {
            return integer + integer2;
        }
    }

    public static void textFile(JavaSparkContext sc) {
        JavaRDD<String> distFile = sc.textFile("pom.xml");
//        JavaRDD<Integer> lineLengths = distFile.map(s -> s.length());
//        JavaRDD<Integer> lineLengths = distFile.map(new GetLength());
        JavaRDD<Integer> lineLengths = distFile.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });
//        lineLengths.persist(StorageLevel.MEMORY_ONLY());
//        int totalLength = lineLengths.reduce((a, b) -> a + b);
//        int totalLength = lineLengths.reduce(new Sum());
        int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println("totalLength:" + totalLength);
    }

    public static void reduceByKey(JavaSparkContext sc) {
        JavaRDD<String> lines = sc.textFile("pom.xml");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        List<Tuple2<String, Integer>> list = counts.take(10);
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }
    }

    public static void broadcast(JavaSparkContext sc) {
        Broadcast<int[]> b = sc.broadcast(new int[]{1, 2, 3});
        System.out.println(b.value());
    }

    //累加器的更新只发生在action操作中，
    public static void accumulator(JavaSparkContext sc) {
        LongAccumulator accum = sc.sc().longAccumulator();
        sc.parallelize(Arrays.asList(1, 2, 3, 4, 5)).foreach(x -> accum.add(x));
        System.out.println(accum.value());
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("mySpark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        textFile(sc);
        reduceByKey(sc);
        broadcast(sc);
        accumulator(sc);
        sc.stop();
    }
}
