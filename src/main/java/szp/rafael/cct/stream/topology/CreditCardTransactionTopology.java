package szp.rafael.cct.stream.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import szp.rafael.cct.model.creditCard.CreditCardTransaction;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;
import szp.rafael.cct.serde.JSONSerdes;
import szp.rafael.cct.serde.SerdeFactory;
import szp.rafael.cct.stream.processor.GeoWindowCheck;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class CreditCardTransactionTopology {

    static Logger logger = org.slf4j.LoggerFactory.getLogger(CreditCardTransactionTopology.class);

    public static final String TRANSACTIONS_TOPIC = "credit-card-transactions";
    public static final String CLIENT_TRANSACTIONS_TOPIC = "account-transactions-by-account";
    public static final String ACCOUNT_BALANCE_TOPIC = "account-balance";
    public static final String GEO_CC_STORE = "geo-cc-store";
    public static final String IP_CC_STORE = "ip-cc-store";
    public static final String PATTERN_CC_STORE = "pattern-cc-store";
    public static final String VELOCITY_CC_STORE = "velocity-cc-store";
    public static final String HIGH_FREQ_CC_STORE = "high-freq-cc-store";
    public static final String PROCESSED_ACCOUNT_TRANSACTIONS_STORE = "processed-account-transactions-store";
    public static final String PROCESSED_CREDIT_CARD_TRANSACTIONS_TOPIC = "processed-credit-card-transactions";


    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        Duration windowSize30min = Duration.ofMinutes(30);

        // Passo 1 - Capturando as transações

        KStream<String, CreditCardTransaction> transactionsStream = builder.stream(TRANSACTIONS_TOPIC, Consumed.with(Serdes.String(), getCreditCardTransactionSerde()));
        transactionsStream
                .selectKey((key, transaction) -> transaction.getClientId())
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


        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> geoStore = createWindowStore(GEO_CC_STORE,windowSize30min);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> ipStore = createWindowStore(IP_CC_STORE,windowSize30min);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> patternStore = createWindowStore(PATTERN_CC_STORE,windowSize30min);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> velocityStore = createWindowStore(VELOCITY_CC_STORE,windowSize30min);
        StoreBuilder<WindowStore<String, ProcessedClientCCTransaction>> highFreqStore = createWindowStore(HIGH_FREQ_CC_STORE,windowSize30min);

        builder.addStateStore(geoStore);
        builder.addStateStore(ipStore);
        builder.addStateStore(patternStore);
        builder.addStateStore(velocityStore);
        builder.addStateStore(highFreqStore);

        //Aspecto importante!!!!!!
        //Estou considerando que esta aplicação pega eventos gerados por processadores anteriores que gravam o transactionTime
        //corretamente. Não é intenção usar ingestion, event, wallclock ou qq outro tipo de time.
        KStream<String, ProcessedClientCCTransaction> processedTransactionStream = transactionsByClientStream
                .process(new GeoWindowCheck(GEO_CC_STORE), GEO_CC_STORE);


        //Passo 3 - Processando as transações e atualizando os saldos

        processedTransactionStream.peek((k, v) -> {
            logger.info("Conta: {}, ProcessedTransaction: {}", k, v.toJSONString());
        });


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
