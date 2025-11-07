package szp.rafael.cct.stream.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import szp.rafael.cct.model.creditCard.CreditCardTransaction;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;
import szp.rafael.cct.serde.JSONDeserializer;
import szp.rafael.cct.serde.JsonSerializer;
import szp.rafael.cct.stream.processor.CCTxMerger;
import szp.rafael.cct.stream.processor.GeoWindowCheck;
import szp.rafael.cct.stream.processor.HighFrequencyWindowCheck;
import szp.rafael.cct.stream.processor.MultipleIPWindowCheck;
import szp.rafael.cct.stream.processor.PatternWindowCheck;
import szp.rafael.cct.stream.processor.VelocityWindowCheck;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class CreditCardTransactionTopologyFinal {

    public static final String FRAUD_AGG_STORE = "fraud-agg-store";
    public static final String CC_TX_MERGE = "cc-tx-merge";
    static Logger logger = org.slf4j.LoggerFactory.getLogger(CreditCardTransactionTopologyFinal.class);

    public static final String TRANSACTIONS_TOPIC = "credit-card-transactions";
    public static final String CLIENT_TRANSACTIONS_TOPIC = "credit-card-transactions-by-client";
    public static final String GEO_CC_STORE = "geo-cc-store";
    public static final String IP_CC_STORE = "ip-cc-store";
    public static final String PATTERN_CC_STORE = "pattern-cc-store";
    public static final String VELOCITY_CC_STORE = "velocity-cc-store";
    public static final String HIGH_FREQ_CC_STORE = "high-freq-cc-store";

    public static final String PROCESSED_CREDIT_CARD_TRANSACTIONS_TOPIC = "processed-credit-card-transactions";
    public static final String REFUSED_CREDIT_CARD_TRANSACTIONS_TOPIC = "refused-credit-card-transactions";


    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        // Passo 1 - Capturando as transações

        KStream<String, CreditCardTransaction> transactionsStream = builder.stream(TRANSACTIONS_TOPIC, Consumed.with(Serdes.String(), getCreditCardTransactionSerde()));
        transactionsStream
                .selectKey((key, transaction) -> transaction.getClientId(), Named.as("cc-tx-by-client"))
                .to(CLIENT_TRANSACTIONS_TOPIC, Produced.with(Serdes.String(), getCreditCardTransactionSerde()));


        //Passo 2 - Como o chaveamento foi feito pela transaction ID, preciso fazer um rekey para um topico correto,
        // para que as transações de cada conta fiquem na mesma partição
        KStream<String, CreditCardTransaction> transactionsByClientStream = builder.stream(CLIENT_TRANSACTIONS_TOPIC, Consumed.with(Serdes.String(), getCreditCardTransactionSerde()));


        Map<String, String> changelogConfig = new HashMap<>();
        changelogConfig.put("min.insync.replicas", "1");

        Duration analysisWindowSize = Duration.ofMinutes(30);

        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> geoStore = createWindowStore(GEO_CC_STORE, analysisWindowSize);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> ipStore = createWindowStore(IP_CC_STORE, analysisWindowSize);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> patternStore = createWindowStore(PATTERN_CC_STORE, analysisWindowSize);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> velocityStore = createWindowStore(VELOCITY_CC_STORE, analysisWindowSize);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> highFreqStore = createWindowStore(HIGH_FREQ_CC_STORE, analysisWindowSize);
        StoreBuilder<KeyValueStore<String, ProcessedClientCCTransaction>> fraudFlagsStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(FRAUD_AGG_STORE),
                Serdes.String(),
                getProcessedClientCCTransactionJSONSerdes()
        );

        builder.addStateStore(geoStore);
        builder.addStateStore(ipStore);
        builder.addStateStore(patternStore);
        builder.addStateStore(velocityStore);
        builder.addStateStore(highFreqStore);
        builder.addStateStore(fraudFlagsStore);

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

        //Aqui resolvi fazer um merge em vez de join, dessa forma evito explosão de cardinalidade
        //Uma melhoria que posso fazer é fazer o merge sem reparticionar 5x (um para cada verificador)
        //A idéia seria reparticionar após o processamento de CCTxMerger, daí seria 1 reparticionamento em vez de 5
        KStream<String, ProcessedClientCCTransaction> geo = geoTransactionStream
                .selectKey((k, n) -> n.getCurrentClientCCTransaction().getTransactionId()) // MUDANÇA: Chave é o ID da Transação
                .repartition(repartitionParams.withName("geo-tx-by-tid"));

        KStream<String, ProcessedClientCCTransaction> ip = ipTransactionStream
                .selectKey((k, tx) -> tx.getCurrentClientCCTransaction().getTransactionId()) // MUDANÇA: Chave é o ID da Transação
                .repartition(repartitionParams.withName("ip-tx-by-tid"));

        KStream<String, ProcessedClientCCTransaction> pattern = patternTransactionStream
                .selectKey((k, tx) -> tx.getCurrentClientCCTransaction().getTransactionId()) // MUDANÇA: Chave é o ID da Transação
                .repartition(repartitionParams.withName("pattern-tx-by-tid"));

        KStream<String, ProcessedClientCCTransaction> velocity = velocityTransactionStream
                .selectKey((k, tx) -> tx.getCurrentClientCCTransaction().getTransactionId()) // MUDANÇA: Chave é o ID da Transação
                .repartition(repartitionParams.withName("velocity-tx-by-tid"));

        KStream<String, ProcessedClientCCTransaction> hf = highFreqTransactionStream
                .selectKey((k, tx) -> tx.getCurrentClientCCTransaction().getTransactionId()) // MUDANÇA: Chave é o ID da Transação
                .repartition(repartitionParams.withName("highfreq-tx-by-tid"));

        //Fazendo merge em vez de join sobretudo porque as streams contem todos os resultados
        // Como nao há filtragem, vamos por esse caminho
        KStream<String, ProcessedClientCCTransaction> unioned = geo
                .merge(ip)
                .merge(pattern)
                .merge(velocity)
                .merge(hf)
                ;



        KStream<String, ProcessedClientCCTransaction> mergeStream = unioned.process(() -> new CCTxMerger(FRAUD_AGG_STORE), FRAUD_AGG_STORE);
        mergeStream.to(CC_TX_MERGE, Produced.with(Serdes.String(), getProcessedClientCCTransactionJSONSerdes()));

//        mergeStream.peek((k,v)-> {
//            logger.debug("mergeStream k {} fraud{}",k,v.getCurrentClientCCTransaction().getTransactionId());
//        });

        KTable<String, ProcessedClientCCTransaction> consolidatedTable = builder.table(CC_TX_MERGE, Consumed.with(Serdes.String(), getProcessedClientCCTransactionJSONSerdes()));

        KStream<String, ProcessedClientCCTransaction> finalStream = consolidatedTable.toStream();

        Map<String, KStream<String, ProcessedClientCCTransaction>> branches = finalStream.split(Named.as("EVALUATED_STREAM-"))
                .branch((k, v) -> v.getFraudScore().compareTo(BigDecimal.ZERO) > 0, Branched.as("FRAUD"))
                .defaultBranch(Branched.as("NOT_FRAUD"));


//        finalStream.peek((key, value) ->{
//            logger.debug("k: {} | fraudScore: {}", key,value.getFraudScore());
//        });

        branches.get("EVALUATED_STREAM-FRAUD").to(REFUSED_CREDIT_CARD_TRANSACTIONS_TOPIC, Produced.with(Serdes.String(), getProcessedClientCCTransactionJSONSerdes()));
        branches.get("EVALUATED_STREAM-NOT_FRAUD").to(PROCESSED_CREDIT_CARD_TRANSACTIONS_TOPIC, Produced.with(Serdes.String(), getProcessedClientCCTransactionJSONSerdes()));


        return builder.build();
    }


    public static Serde<CreditCardTransaction> getCreditCardTransactionSerde() {
        Serde<CreditCardTransaction> txSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JSONDeserializer<>(CreditCardTransaction.class));
        return txSerde;
    }

    public static Serde<ProcessedClientCCTransaction> getProcessedClientCCTransactionJSONSerdes() {
        Serde<ProcessedClientCCTransaction> txSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JSONDeserializer<>(ProcessedClientCCTransaction.class));
        return txSerde;
    }

    public static StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> createWindowStore(String storeName, Duration windowSize) {
        Map<String, String> changelogConfig = new HashMap<>();
        changelogConfig.put("min.insync.replicas", "1");
        changelogConfig.put("segment.bytes", "67108864 ");//64Mb - Afim de fazer compaction mais rapido
        changelogConfig.put("compression.type", "lz4 ");
        changelogConfig.put("min.cleanable.dirty.ratio", "0.35 ");

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
