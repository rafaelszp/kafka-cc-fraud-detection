package szp.rafael.cct;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import szp.rafael.cct.model.AccountTransaction;
import szp.rafael.cct.serde.JSONSerdes;
import szp.rafael.cct.stream.topology.CreditCardTransactionTopology;

import java.util.Properties;
import java.util.Random;


public class MainApp {

    static Logger logger = org.slf4j.LoggerFactory.getLogger(MainApp.class);


    public static void main(String[] args) {
        System.out.println("Hello, Kafka Account Transaction!");

        Random random = new Random();
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processador-bancario-app-"+ random.nextInt(Integer.MAX_VALUE));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerdes.class.getName());
        props.put("JsonPOJOClass", AccountTransaction.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaStreams streams = new KafkaStreams(CreditCardTransactionTopology.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();

    }
}
