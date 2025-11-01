package szp.rafael.cct.stream.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import szp.rafael.cct.model.creditCard.CreditCardTransaction;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;
import szp.rafael.cct.serde.JSONSerdes;
import szp.rafael.cct.serde.SerdeFactory;
import szp.rafael.cct.stream.join.ProcessedClientCCJoiner;
import szp.rafael.cct.stream.processor.GeoWindowCheck;
import szp.rafael.cct.stream.processor.HighFrequencyWindowCheck;
import szp.rafael.cct.stream.processor.MultipleIPWindowCheck;
import szp.rafael.cct.stream.processor.PatternWindowCheck;
import szp.rafael.cct.stream.processor.VelocityWindowCheck;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class CreditCardTransactionTopology {

    static Logger logger = org.slf4j.LoggerFactory.getLogger(CreditCardTransactionTopology.class);

    public static final String TRANSACTIONS_TOPIC = "credit-card-transactions";
    public static final String CLIENT_TRANSACTIONS_TOPIC = "credit-card-transactions-by-client";
    public static final String GEO_CC_STORE = "geo-cc-store";
    public static final String IP_CC_STORE = "ip-cc-store";
    public static final String PATTERN_CC_STORE = "pattern-cc-store";
    public static final String VELOCITY_CC_STORE = "velocity-cc-store";
    public static final String HIGH_FREQ_CC_STORE = "high-freq-cc-store";
    public static final String FRAUD_FLAGS_TOPIC = "fraud-flags-by-txid";
    public static final String FRAUD_FLAGS_STORE = "fraud-flags-by-txid-store";

    public static final String PROCESSED_CREDIT_CARD_TRANSACTIONS_TOPIC = "processed-credit-card-transactions";
    public static final String REFUSED_CREDIT_CARD_TRANSACTIONS_TOPIC = "refused-credit-card-transactions";


    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        Duration windowSize30min = Duration.ofMinutes(30);

        // Passo 1 - Capturando as transações

        KStream<String, CreditCardTransaction> transactionsStream = builder.stream(TRANSACTIONS_TOPIC, Consumed.with(Serdes.String(), getCreditCardTransactionSerde()));
        transactionsStream
                .selectKey((key, transaction) -> transaction.getClientId(), Named.as("cc-tx-by-client"))
                .to(CLIENT_TRANSACTIONS_TOPIC, Produced.with(Serdes.String(), getCreditCardTransactionSerde()));


        //Passo 2 - Como o chaveamento foi feito pela transaction ID, preciso fazer um rekey para um topico correto,
        // para que as transações de cada conta fiquem na mesma partição
        KStream<String, CreditCardTransaction> transactionsByClientStream = builder.stream(CLIENT_TRANSACTIONS_TOPIC, Consumed.with(Serdes.String(), getCreditCardTransactionSerde()));


//        transactionsByClientStream.peek((key, transaction) -> {
//            logger.info("\n\n\n\n\n");
//            logger.info("###############################################################################################################");
//            logger.info("Conta: {}, Transação Recebida: {}", key, transaction.toJSONString());
//        });

        Map<String, String> changelogConfig = new HashMap<>();
        changelogConfig.put("min.insync.replicas", "1");

        Duration joinWindowSize = Duration.ofMinutes(1);

        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> geoStore = createWindowStore(GEO_CC_STORE, joinWindowSize);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> ipStore = createWindowStore(IP_CC_STORE, joinWindowSize);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> patternStore = createWindowStore(PATTERN_CC_STORE, joinWindowSize);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> velocityStore = createWindowStore(VELOCITY_CC_STORE, joinWindowSize);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> highFreqStore = createWindowStore(HIGH_FREQ_CC_STORE, joinWindowSize);

        builder.addStateStore(geoStore);
        builder.addStateStore(ipStore);
        builder.addStateStore(patternStore);
        builder.addStateStore(velocityStore);
        builder.addStateStore(highFreqStore);

        //Aspecto importante!!!!!!
        //Estou considerando que esta aplicação pega eventos gerados por processadores anteriores que gravam o transactionTime
        //corretamente. Não é intenção usar ingestion, event, wallclock ou qq outro tipo de time.
        KStream<String, ProcessedClientCCTransaction> geoTransactionStream = transactionsByClientStream
                .process(new GeoWindowCheck(GEO_CC_STORE), GEO_CC_STORE);

        KStream<String, ProcessedClientCCTransaction> ipTransactionStream = transactionsByClientStream
                .process(new MultipleIPWindowCheck(IP_CC_STORE), IP_CC_STORE);

        KStream<String, ProcessedClientCCTransaction> patternTransactionStream = transactionsByClientStream
                .process(new PatternWindowCheck(PATTERN_CC_STORE), PATTERN_CC_STORE);

        KStream<String, ProcessedClientCCTransaction> velocityTransactionStream = transactionsByClientStream
                .process(new VelocityWindowCheck(VELOCITY_CC_STORE), VELOCITY_CC_STORE);

        KStream<String, ProcessedClientCCTransaction> highFreqTransactionStream = transactionsByClientStream
                .process(new HighFrequencyWindowCheck(HIGH_FREQ_CC_STORE), HIGH_FREQ_CC_STORE);


        //Agora vamos fazer join
        Repartitioned<String, ProcessedClientCCTransaction> repartitionParams =
                Repartitioned.with(Serdes.String(), getProcessedClientCCTransactionJSONSerdes());
        ProcessedClientCCJoiner joiner = new ProcessedClientCCJoiner();
        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(5));
        StreamJoined<String, ProcessedClientCCTransaction, ProcessedClientCCTransaction> joinWith = StreamJoined.with(Serdes.String(),
                getProcessedClientCCTransactionJSONSerdes(),
                getProcessedClientCCTransactionJSONSerdes());

        //Aqui foi necessário reparticionar para uma nova chave, a transactionId
        //Se eu mantivesse o clientId, a cada join da janela ele irá gerar L x R registros sendo número de Left * Número de Right
        //Isso é tão verdade que quando gerei 7 transações, ao final ele gerou 3157 registros

        KStream<String, ProcessedClientCCTransaction> geo = geoTransactionStream
                .selectKey((k, n) -> n.getCurrentClientCCTransaction().getTransactionId(), Named.as("geo-tx-by-tid")) // MUDANÇA: Chave é o ID da Transação
                .repartition(repartitionParams);

        KStream<String, ProcessedClientCCTransaction> ip = ipTransactionStream
                .selectKey((k, tx) -> tx.getCurrentClientCCTransaction().getTransactionId(), Named.as("ip-tx-by-tid")) // MUDANÇA: Chave é o ID da Transação
                .repartition(repartitionParams);

        KStream<String, ProcessedClientCCTransaction> pattern = patternTransactionStream
                .selectKey((k, tx) -> tx.getCurrentClientCCTransaction().getTransactionId(), Named.as("pattern-tx-by-tid")) // MUDANÇA: Chave é o ID da Transação
                .repartition(repartitionParams);

        KStream<String, ProcessedClientCCTransaction> velocity = velocityTransactionStream
                .selectKey((k, tx) -> tx.getCurrentClientCCTransaction().getTransactionId(), Named.as("velocity-tx-by-tid")) // MUDANÇA: Chave é o ID da Transação
                .repartition(repartitionParams);

        KStream<String, ProcessedClientCCTransaction> hf = highFreqTransactionStream
                .selectKey((k, tx) -> tx.getCurrentClientCCTransaction().getTransactionId(), Named.as("highfreq-tx-by-tid")) // MUDANÇA: Chave é o ID da Transação
                .repartition(repartitionParams);

//        geoTransactionStream.peek((k,v) -> logger.info("GEO in: key={} ts={}", k, v.getCurrentClientCCTransaction().getTimestamp()));
//        ip.peek((k,v) -> logger.info("IP in: key={} ts={}", k, v.getCurrentClientCCTransaction().getTimestamp()));


        KStream<String, ProcessedClientCCTransaction> joined = geo
                .join(ip, joiner, joinWindows, joinWith)
                .join(pattern, joiner, joinWindows, joinWith)
                .join(velocity, joiner, joinWindows, joinWith)
                .join(hf, joiner, joinWindows, joinWith)
                ;


        KStream<String, ProcessedClientCCTransaction> acceptedStream  = joined.filterNot((k, tx) -> tx.getFraudScore().compareTo(BigDecimal.ZERO) > 0);
        KStream<String, ProcessedClientCCTransaction> refusedStream = joined.filter((k, tx) -> tx.getFraudScore().compareTo(BigDecimal.ZERO) > 0);


//        joined.peek((k, v) -> {
//            logger.info("Joined Conta: {}, FraudScore: {}, Window size: {}", k, v.getFraudScore(), v.getLastCCTransactions().size());
//        });

        refusedStream.to(REFUSED_CREDIT_CARD_TRANSACTIONS_TOPIC, Produced.with(Serdes.String(), getProcessedClientCCTransactionJSONSerdes()));
        acceptedStream.to(PROCESSED_CREDIT_CARD_TRANSACTIONS_TOPIC, Produced.with(Serdes.String(), getProcessedClientCCTransactionJSONSerdes()));

        //Passo 3 - Processando as transações e atualizando os saldos


//        acceptedStream.peek((k, v) -> {
//            logger.info("Accepted Conta: {}, FraudScore: {}", k, v.getFraudScore());
//        });


        return builder.build();
    }


    public static JSONSerdes<CreditCardTransaction> getCreditCardTransactionSerde() {
        return new SerdeFactory<CreditCardTransaction>().createSerde(CreditCardTransaction.class);
    }

    public static JSONSerdes<ProcessedClientCCTransaction> getProcessedClientCCTransactionJSONSerdes() {
        return new SerdeFactory<ProcessedClientCCTransaction>().createSerde(ProcessedClientCCTransaction.class);
    }

    public static StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> createWindowStore(String storeName, Duration windowSize) {
        Map<String, String> changelogConfig = new HashMap<>();
        changelogConfig.put("min.insync.replicas", "1");

        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> store =
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore(
                                storeName,                                      // nome do store
                                windowSize.plus(5, ChronoUnit.MINUTES),    // retenção total
                                windowSize,                                            // tamanho da janela
                                false                                                       // retain duplicates? (false = 1 valor por chave+janela)
                        ),
                        Serdes.String(),
                        getProcessedClientCCTransactionJSONSerdes()
                ).withLoggingEnabled(changelogConfig);
        return store;

    }


}
