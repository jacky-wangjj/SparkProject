package sparkKafka;

import kafka.serializer.StringDecoder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
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
import parquet.org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 将 Spark Streaming + Kafka direct 的 offset 保存进入Zookeeper
 * https://blog.csdn.net/sun_qiangwei/article/details/52089795
 * @author jacky-wangjj
 * @date 2020/6/3
 */
public class SparkStreamingOnKafkaDirectSaveInZookeeper {

    public static JavaStreamingContext createContext() {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingOnKafkaDirectSaveInZookeeper");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","127.0.0.1:9092");

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

        lines.foreachRDD(new VoidFunction<JavaPairRDD<String,String>>(){
            @Override
            public void call(JavaPairRDD<String, String> t) throws Exception {

                ObjectMapper objectMapper = new ObjectMapper();

                CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                        .connectString("192.168.1.151:2181").connectionTimeoutMs(1000)
                        .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();

                curatorFramework.start();

                for (OffsetRange offsetRange : offsetRanges.get()) {
                    final byte[] offsetBytes = objectMapper.writeValueAsBytes(offsetRange.untilOffset());
                    String nodePath = "/consumers/spark-group/offsets/" + offsetRange.topic()+ "/" + offsetRange.partition();
                    if(curatorFramework.checkExists().forPath(nodePath)!=null){
                        curatorFramework.setData().forPath(nodePath,offsetBytes);
                    }else{
                        curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
                    }
                }
                curatorFramework.close();
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
