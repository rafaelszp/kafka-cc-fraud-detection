package szp.rafael.cct;


import com.github.f4b6a3.ulid.UlidCreator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import szp.rafael.cct.model.creditCard.CardDetails;
import szp.rafael.cct.model.creditCard.CreditCardTransaction;
import szp.rafael.cct.model.creditCard.Geolocation;
import szp.rafael.cct.model.creditCard.IpData;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;
import szp.rafael.cct.stream.topology.CreditCardTransactionTopologyFinal;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class MainAppTestFinal {

    Properties streamProps = new Properties();

    @BeforeEach
    public void setup(){
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-" + UUID.randomUUID());

        streamProps.put("auto.offset.reset", "earliest");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerdes.class.getName());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        streamProps.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test-"+UUID.randomUUID());

    }




    @Test
    public void should_process_account_transactions(){

        Topology build = CreditCardTransactionTopologyFinal.build();
        System.out.println(build.describe());
        try (final TopologyTestDriver testDriver = new TopologyTestDriver(build, streamProps)) {
            final TestInputTopic<String, CreditCardTransaction> ccTransactions = testDriver.createInputTopic(
                    CreditCardTransactionTopologyFinal.TRANSACTIONS_TOPIC,
                    Serdes.String().serializer(),
                    CreditCardTransactionTopologyFinal.getCreditCardTransactionSerde().serializer()
            );

            String accountId = "ACC_01";
            CardDetails cardDetails = new CardDetails("1234-5678-9012-3456","teste","123", "12/25");
            Geolocation goianiaGeo = new Geolocation(-16.665136d, -49.286041d, "0id");
            Geolocation saoPauloGeo = new Geolocation(-23.550519d, -46.633309d, "1id");

            IpData ipData = new IpData("200.241.235.123");
            ;
            Instant now = Instant.ofEpochMilli(1761937000000L); //2025-10-31T18:56:40Z
            final List<CreditCardTransaction> transactions = List.of(
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(10d), now.toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(15d), now.plus(5,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(25d), now.plus(10,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(40d), now.plus(15,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase(),accountId,BigDecimal.valueOf(60d), now.plus(17,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase()+"_1_FRD",accountId,BigDecimal.valueOf(120d), now.plus(60,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,goianiaGeo,ipData),
                    new CreditCardTransaction(UlidCreator.getMonotonicUlid().toLowerCase()+"_2_FRD",accountId,BigDecimal.valueOf(190d), now.plus(64,ChronoUnit.MINUTES).toEpochMilli(),cardDetails,saoPauloGeo,ipData)
            );

            transactions.forEach(txn-> ccTransactions.pipeInput(txn.getTransactionId(),txn,txn.getTimestamp()));


            final TestOutputTopic<String, ProcessedClientCCTransaction> processedAccountTransactionsTopic = testDriver.createOutputTopic(
                    CreditCardTransactionTopologyFinal.PROCESSED_CREDIT_CARD_TRANSACTIONS_TOPIC,
                    Serdes.String().deserializer(),
                    CreditCardTransactionTopologyFinal.getProcessedClientCCTransactionJSONSerdes().deserializer()
            );

            final TestOutputTopic<String, ProcessedClientCCTransaction> refusedTransactionsTopic = testDriver.createOutputTopic(
                    CreditCardTransactionTopologyFinal.REFUSED_CREDIT_CARD_TRANSACTIONS_TOPIC,
                    Serdes.String().deserializer(),
                    CreditCardTransactionTopologyFinal.getProcessedClientCCTransactionJSONSerdes().deserializer()
            );
            final int expectedRejectedCount = 2;
            final int expectedCompletedCount = transactions.size() - expectedRejectedCount;

            testDriver.advanceWallClockTime(Duration.of(3, ChronoUnit.HOURS));

            List<ProcessedClientCCTransaction> processedTransactions = processedAccountTransactionsTopic.readValuesToList();
            List<ProcessedClientCCTransaction> refusedTransactions = refusedTransactionsTopic.readValuesToList();

            int processedCount = processedTransactions.size();
            System.out.println("processedTransactions = " + processedCount);
            processedTransactions.forEach(proc-> System.out.println("ac = " + proc.getCurrentClientCCTransaction().getTransactionId()+" fs:  "+proc.getFraudScore().doubleValue()));

            int refusedCount = refusedTransactions.size();
            System.out.println("refusedTransactions   = " + refusedCount);
            refusedTransactions.forEach(rfs-> System.out.println("rf = " + rfs.getCurrentClientCCTransaction().getTransactionId()+" fs:  "+rfs.getFraudScore().doubleValue()
                    +rfs.getLastCCTransactions().stream().map(CreditCardTransaction::getTransactionId).collect(Collectors.joining(", "))));

            assertEquals(expectedCompletedCount, processedCount);
            assertEquals(expectedRejectedCount, refusedCount);

        }

    }

}
