package edu.illinois.sketchbench.datagen;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * The class for sending messages to Kafka.
 */
public class KafkaMessageSender {

    private static String BOOTSTRAP_SERVERS;
    private static String TOPIC1;
    private static final Logger log = Logger.getLogger(KafkaMessageSender.class);

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
    public static Producer<String, String> createProducer(String kafkaEndpoint, String kafkaTopic) {
        Properties props = new Properties();
        TOPIC1 = kafkaTopic;

        log.info("kafka endpoint - "+kafkaEndpoint);
        log.info("kafka topic - "+kafkaTopic);

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaEndpoint);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void runProducer1(String jSON,Producer producer) throws Exception {

        //final Producer<String, String> producer = createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC1, jSON);

        try {
            producer.send(record, null);
            producer.flush();
            log.info("Message sent to kafka topic of " + TOPIC1);
        } finally {
            producer.flush();
            //producer.close();
        }
    }

//    public static void runProducer2(String jSON,Producer producer) throws Exception {
//
//        //final Producer<String, String> producer = createProducer();
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC2, jSON);
//
//        try {
//            producer.send(record, null);
//            producer.flush();
//            //log.info("Message sent to kafka topic of " + TOPIC);
//        } finally {
//            producer.flush();
//            //producer.close();
//        }
//    }
//
//    public static void runProducer3(String jSON,Producer producer) throws Exception {
//
//        //final Producer<String, String> producer = createProducer();
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC3, jSON);
//
//        try {
//            producer.send(record, null);
//            producer.flush();
//            //log.info("Message sent to kafka topic of " + TOPIC);
//        } finally {
//            producer.flush();
//            //producer.close();
//        }
//    }
//    public static void runProducer4(String jSON,Producer producer) throws Exception {
//
//        //final Producer<String, String> producer = createProducer();
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC4, jSON);
//
//        try {
//            producer.send(record, null);
//            producer.flush();
//            //log.info("Message sent to kafka topic of " + TOPIC);
//        } finally {
//            producer.flush();
//            //producer.close();
//        }
//    }
}
