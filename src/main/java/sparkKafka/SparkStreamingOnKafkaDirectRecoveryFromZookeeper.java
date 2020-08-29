package sparkKafka;

import com.google.common.collect.ImmutableMap;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import parquet.org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Spark Streaming + Kafka direct 从Zookeeper中恢复offset
 * https://blog.csdn.net/sun_qiangwei/article/details/52098917
 *
 * @author jacky-wangjj
 * @date 2020/6/3
 */
public class SparkStreamingOnKafkaDirectRecoveryFromZookeeper {
    public static JavaStreamingContext createContext() {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingOnKafkaDirect");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(30));
        jsc.checkpoint("/checkpoint");

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "192.168.1.151:1234,192.168.1.151:1235,192.168.1.151:1236");

        Map<TopicAndPartition, Long> topicOffsets = getTopicOffsets("192.168.1.151:1234,192.168.1.151:1235,192.168.1.151:1236", "kafka_direct");

        Map<TopicAndPartition, Long> consumerOffsets = getConsumerOffsets("192.168.1.151:2181", "spark-group", "kafka_direct");
        if (null != consumerOffsets && consumerOffsets.size() > 0) {
            //key相同会覆盖value
            topicOffsets.putAll(consumerOffsets);
        }

//        for(Map.Entry<TopicAndPartition, Long> item:topicOffsets.entrySet()){
//            item.setValue(0l);
//        }

        for (Map.Entry<TopicAndPartition, Long> entry : topicOffsets.entrySet()) {
            System.out.println(entry.getKey().topic() + "\t" + entry.getKey().partition() + "\t" + entry.getValue());
        }

        JavaInputDStream<String> lines = KafkaUtils.createDirectStream(jsc,
                String.class, String.class, StringDecoder.class,
                StringDecoder.class, String.class, kafkaParams,
                topicOffsets, new Function<MessageAndMetadata<String, String>, String>() {

                    @Override
                    public String call(MessageAndMetadata<String, String> v1)
                            throws Exception {
                        return v1.message();
                    }
                });


        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        JavaDStream<String> words = lines.transform(
                new Function<JavaRDD<String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);
                        return rdd;
                    }
                }
        ).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(
                    String event)
                    throws Exception {
                return (Iterator<String>) Arrays.asList(event);
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

        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> t) throws Exception {

                ObjectMapper objectMapper = new ObjectMapper();

                CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                        .connectString("192.168.1.151:2181").connectionTimeoutMs(1000)
                        .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();

                curatorFramework.start();

                for (OffsetRange offsetRange : offsetRanges.get()) {
                    final byte[] offsetBytes = objectMapper.writeValueAsBytes(offsetRange.untilOffset());
                    String nodePath = "/consumers/spark-group/offsets/" + offsetRange.topic() + "/" + offsetRange.partition();
                    if (curatorFramework.checkExists().forPath(nodePath) != null) {
                        curatorFramework.setData().forPath(nodePath, offsetBytes);
                    } else {
                        curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
                    }
                }

                curatorFramework.close();
            }

        });

        wordsCount.print();

        return jsc;
    }


    public static Map<TopicAndPartition, Long> getConsumerOffsets(String zkServers,
                                                                  String groupID, String topic) {
        Map<TopicAndPartition, Long> retVals = new HashMap<TopicAndPartition, Long>();

        ObjectMapper objectMapper = new ObjectMapper();
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zkServers).connectionTimeoutMs(1000)
                .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();

        curatorFramework.start();

        try {
            String nodePath = "/consumers/" + groupID + "/offsets/" + topic;
            if (curatorFramework.checkExists().forPath(nodePath) != null) {
                List<String> partitions = curatorFramework.getChildren().forPath(nodePath);
                for (String partiton : partitions) {
                    int partitionL = Integer.valueOf(partiton);
                    Long offset = objectMapper.readValue(curatorFramework.getData().forPath(nodePath + "/" + partiton), Long.class);
                    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionL);
                    retVals.put(topicAndPartition, offset);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        curatorFramework.close();

        return retVals;
    }

    public static Map<TopicAndPartition, Long> getTopicOffsets(String zkServers, String topic) {
        Map<TopicAndPartition, Long> retVals = new HashMap<TopicAndPartition, Long>();

        for (String zkServer : zkServers.split(",")) {
            SimpleConsumer simpleConsumer = new SimpleConsumer(zkServer.split(":")[0],
                    Integer.valueOf(zkServer.split(":")[1]),
                    10000,
                    1024,
                    "consumer");
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Arrays.asList(topic));
            TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

            for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
                for (PartitionMetadata part : metadata.partitionsMetadata()) {
                    Broker leader = part.leader();
                    if (leader != null) {
                        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, part.partitionId());

                        PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 10000);
                        OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());
                        OffsetResponse offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest);

                        if (!offsetResponse.hasError()) {
                            long[] offsets = offsetResponse.offsets(topic, part.partitionId());
                            retVals.put(topicAndPartition, offsets[0]);
                        }
                    }
                }
            }
            simpleConsumer.close();
        }
        return retVals;
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
