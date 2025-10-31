package szp.rafael.cct;


import com.github.f4b6a3.ulid.UlidCreator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import szp.rafael.cct.model.creditCard.CardDetails;
import szp.rafael.cct.model.creditCard.CreditCardTransaction;
import szp.rafael.cct.model.creditCard.Geolocation;
import szp.rafael.cct.model.creditCard.IpData;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;
import szp.rafael.cct.serde.JSONSerdes;
import szp.rafael.cct.stream.topology.CreditCardTransactionTopology;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.UUID;


public class MainAppTest {

    Properties streamProps = new Properties();

    @BeforeEach
    public void setup(){
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-" + UUID.randomUUID());

        streamProps.put("auto.offset.reset", "earliest");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerdes.class.getName());
        streamProps.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test-"+UUID.randomUUID());

    }




    @Test
    public void should_process_account_transactions(){

        Topology build = CreditCardTransactionTopology.build();
        System.out.println(build.describe());
        try (final TopologyTestDriver testDriver = new TopologyTestDriver(build, streamProps)) {
            final TestInputTopic<String, CreditCardTransaction> ccTransactions = testDriver.createInputTopic(
                    CreditCardTransactionTopology.TRANSACTIONS_TOPIC,
                    Serdes.String().serializer(),
                    CreditCardTransactionTopology.getCreditCardTransactionSerde().serializer()
            );
            final WindowStore<String, ProcessedClientCCTransaction> store = testDriver.getWindowStore(CreditCardTransactionTopology.GEO_CC_STORE);

            String accountId = "ACC_01";
            CardDetails cardDetails = new CardDetails("1234-5678-9012-3456","teste","123", "12/25");
            Geolocation goianiaGeo = new Geolocation(-16.665136d, -49.286041d, "0id");
            Geolocation saoPauloGeo = new Geolocation(-23.550519d, -46.633309d, "1id");

            IpData ipData = new IpData("200.241.235.123");
            ;
            Instant now = Instant.ofEpochMilli(1761937000000L);
            final List<CreditCardTransaction> transactions = List.of(
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(10d), now.toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(15d), now.plus(5,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(25d), now.plus(10,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(40d), now.plus(15,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(60d), now.plus(20,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(120d), now.plus(60,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(190d), now.plus(70,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,saoPauloGeo,ipData)
            );

            transactions.forEach(txn-> ccTransactions.pipeInput(txn.getTransactionId(),txn,txn.getTimestamp()));

            final BigDecimal expectedValue = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_EVEN);


            testDriver.advanceWallClockTime(Duration.of(2, ChronoUnit.HOURS));

//            ProcessedClientCCTransaction transaction = store.fetch(accountId,now.minusSeconds(3600),now.plusSeconds(3600)).next().value;
//            Assertions.assertEquals(expectedValue,transaction.getFraudScore());

            final TestOutputTopic<String, ProcessedClientCCTransaction> processedAccountTransactionsTopic = testDriver.createOutputTopic(
                    CreditCardTransactionTopology.PROCESSED_CREDIT_CARD_TRANSACTIONS_TOPIC,
                    Serdes.String().deserializer(),
                    CreditCardTransactionTopology.getProcessedClientCCTransactionJSONSerdes().deserializer()
            );
            final int expectedRejectedCount = 1;
            final int expectedCompletedCount = transactions.size() - expectedRejectedCount;

            List<ProcessedClientCCTransaction> processedTransactions = processedAccountTransactionsTopic.readValuesToList();
            System.out.println("processedTransactions = " + processedTransactions.size());
//            Assertions.assertEquals(expectedCompletedCount,processedTransactions.stream().filter(pt-> AccountTransaction.TransactionStatus.COMPLETED.equals(pt.getTransaction().getStatus())).count());
//            Assertions.assertEquals(expectedRejectedCount,processedTransactions.stream().filter(pt-> AccountTransaction.TransactionStatus.REJECTED.equals(pt.getTransaction().getStatus())).count());


        }



    }

}
