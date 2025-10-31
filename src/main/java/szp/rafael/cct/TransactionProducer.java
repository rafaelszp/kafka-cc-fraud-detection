package szp.rafael.cct;

import com.github.f4b6a3.ulid.Ulid;
import com.github.f4b6a3.ulid.UlidCreator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import szp.rafael.cct.model.AccountTransaction;
import szp.rafael.cct.serde.JsonSerializer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class TransactionProducer {

    private static final String TOPIC = "account-transactions";


    public static void main(String[] args) {
        System.out.println("Hello, Kafka Transaction Producer!");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Confirmação de recebimento
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        JsonSerializer<AccountTransaction> accountTransactionJsonSerializer = new JsonSerializer<>();

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, accountTransactionJsonSerializer.getClass().getName());

        String accountId = "ACC_"+ UlidCreator.getMonotonicUlid().toLowerCase();

        try (Producer<String, AccountTransaction> producer = new KafkaProducer<>(props)) {
            Random random = new Random();

            for (long i = 0; i < 150; i++) {
                Ulid hashUlid = UlidCreator.getHashUlid(Instant.now().toEpochMilli() + i, accountId);
                String transactionId = hashUlid.toString();;
                AccountTransaction.TransactionType type = random.nextBoolean() ? AccountTransaction.TransactionType.CREDIT : AccountTransaction.TransactionType.DEBIT;
                AccountTransaction accountTransaction = new AccountTransaction();
                accountTransaction.setAccountId(accountId);
                accountTransaction.setTransactionId(transactionId);
                accountTransaction.setTransactionType(type);
                accountTransaction.setValue(BigDecimal.valueOf(random.nextDouble() * 10000).setScale(2, RoundingMode.HALF_UP));
                accountTransaction.setProcessedTimestamp(Instant.now().toEpochMilli()+1);
                ProducerRecord<String, AccountTransaction> record = new ProducerRecord<>(TOPIC, transactionId, accountTransaction);
                producer.send(record, ((metadata, exception) -> {
                    if (exception == null) {
                        // Mensagem enviada com sucesso!
                        System.out.printf("Mensagem enviada com sucesso! Tópico: %s, Partição: %d, Offset: %d%n",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        // Ocorreu um erro
                        System.err.println("Erro ao enviar mensagem: " + exception.getMessage());
                    }
                }));
                try {
                    Thread.sleep(Math.min(10,i));
                } catch (InterruptedException e) {

                }
            }
            producer.flush();
            System.out.println("\n5 mensagens de compra foram enviadas para o tópico '" + TOPIC + "'.");


        }


    }
}
