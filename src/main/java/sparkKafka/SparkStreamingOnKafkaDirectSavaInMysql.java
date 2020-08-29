package sparkKafka;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * "Spark Streaming + Kafka direct + checkpoints + 代码改变" 引发的问题
 * https://blog.csdn.net/sun_qiangwei/article/details/52080147
 *
 * @author jacky-wangjj
 * @date 2020/6/3
 */
public class SparkStreamingOnKafkaDirectSavaInMysql {
    public static JavaStreamingContext createContext() {
        final Map<String, String> params = new HashMap<String, String>();
        params.put("driverClassName", "com.mysql.jdbc.Driver");
        params.put("url", "jdbc:mysql://192.168.1.151:3306/hive");
        params.put("username", "hive");
        params.put("password", "hive");

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingOnKafkaDirectSavaInMysql");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(30));
        jsc.checkpoint("/checkpoint");

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "127.0.0.1:9092");

        Set<String> topics = new HashSet<String>();
        topics.add("kafka_direct");

        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jsc, String.class,
                String.class, StringDecoder.class,
                StringDecoder.class, kafkaParams,
                topics);

        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        JavaDStream<String> words = lines.transformToPair(
                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                    @Override
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);
                        return rdd;
                    }
                }
        ).flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(
                    Tuple2<String, String> event)
                    throws Exception {
                String line = event._2;
                return (Iterator<String>) Arrays.asList(line);
            }
        });

        JavaPairDStream<String, Integer> pairs = words
                .mapToPair(new PairFunction<String, String, Integer>() {

                    @Override
                    public Tuple2<String, Integer> call(
                            String word) throws Exception {
                        return new Tuple2<String, Integer>(
                                word, 1);
                    }
                });

        JavaPairDStream<String, Integer> wordsCount = pairs
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2)
                            throws Exception {
                        return v1 + v2;
                    }
                });

        lines.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> t) throws Exception {
                DataSource ds = DruidDataSourceFactory.createDataSource(params);
                Connection conn = ds.getConnection();
                Statement stmt = conn.createStatement();
                for (OffsetRange offsetRange : offsetRanges.get()) {
                    stmt.executeUpdate("update kafka_offsets set offset ='"
                            + offsetRange.untilOffset() + "'  where topic='"
                            + offsetRange.topic() + "' and partition='"
                            + offsetRange.partition() + "'");
                }
                conn.close();
            }

        });

        wordsCount.print();

        return jsc;
    }

    public static void main(String[] args) throws InterruptedException {
//        JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
//            @Override
//            public JavaStreamingContext create() {
//                return createContext();
//            }
//        };
//
//        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate("/checkpoint", (Function0<JavaStreamingContext>) factory);
        JavaStreamingContext jsc = createContext();

        jsc.start();

        jsc.awaitTermination();
        jsc.close();
    }
}
