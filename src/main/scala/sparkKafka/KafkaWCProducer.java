package sparkKafka;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author jacky-wangjj
 * @date 2020/6/3
 */
public class KafkaWCProducer {
    private Producer<String, String> producer;
    public final static String TOPIC = "test";

    private KafkaWCProducer() {
        // 设置配置属性
        Properties props = new Properties();
        // 配置kafka的IP和端口
        props.put("metadata.broker.list", "127.0.0.1:9092");
        // 配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // 配置key的序列化类key.serializer.class默认为serializer.class
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // 可选配置，如果不配置，则使用默认的partitioner
        //props.put("partitioner.class", "com.kafka.demo.PartitionerDemo");
        // 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
        // 值为0,1,-1,可以参考
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        // 创建producer
        producer = new Producer<String, String>(config);
    }

    void produce() {
        // 产生并发送消息
        int messageNo = 1000;
        while (true) {
            String key = String.valueOf(messageNo);
            String data = "spark kafka receiver checkpoint or direct zookeeper" + key;
            KeyedMessage<String, String> msg = new KeyedMessage<String, String>(TOPIC, key, data);
            producer.send(msg);
            System.out.println(data);
            messageNo++;
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new KafkaWCProducer().produce();
    }
}
