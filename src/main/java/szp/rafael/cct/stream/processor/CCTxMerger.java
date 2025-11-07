package szp.rafael.cct.stream.processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;

import java.math.BigDecimal;
import java.time.Duration;

public class CCTxMerger implements Processor<String, ProcessedClientCCTransaction,String, ProcessedClientCCTransaction> {

    public static final int GRACE_WINDOW_SIZE_MINUTES = 5; //quanto tempo iremos aguardar processamentos anteriores para que possamos capturar os scores de fraude
    private KeyValueStore<String,ProcessedClientCCTransaction> store;
    private ProcessorContext<String, ProcessedClientCCTransaction> context;
    private final String fraudAggStore;

    public CCTxMerger(String fraudAggStore) {
        this.fraudAggStore = fraudAggStore;
    }

    @Override
    public void init(ProcessorContext<String, ProcessedClientCCTransaction> context) {
        this.context = context;
        store = context.getStateStore(fraudAggStore);
        this.context.schedule(
                Duration.ofMinutes(GRACE_WINDOW_SIZE_MINUTES),
                PunctuationType.WALL_CLOCK_TIME,
                timestamp -> punctuate()
        );
    }

    @Override
    public void process(Record<String, ProcessedClientCCTransaction> record) {
        BigDecimal fraudScore = record.value().getFraudScore();
        ProcessedClientCCTransaction transaction = new ProcessedClientCCTransaction(record.value().getClientId(), record.value().getCurrentClientCCTransaction(), record.value().getLastCCTransactions());
        transaction.setFraudScore(fraudScore);
        ProcessedClientCCTransaction current = store.get(record.key());
        if (current != null) {
            BigDecimal newScore = transaction.getFraudScore().add(current.getFraudScore());
            transaction.setFraudScore(newScore);
            transaction.getLastCCTransactions().forEach(tx-> updateRelatedTxFraudScore(tx.getTransactionId(), newScore));
        }
        store.put(record.key(),transaction);
    }

    public void punctuate(){
        try(KeyValueIterator<String, ProcessedClientCCTransaction> iterator = store.all()){
            while (iterator.hasNext()){
                KeyValue<String, ProcessedClientCCTransaction> next = iterator.next();
                context.forward(new Record<>(next.key, next.value, next.value.getCurrentClientCCTransaction().getTimestamp()));
                store.delete(next.key);
            }
        }
    }

    public void updateRelatedTxFraudScore(String key, BigDecimal newScore){
        ProcessedClientCCTransaction current = store.get(key);
        if(current !=null){
            ProcessedClientCCTransaction transaction = new ProcessedClientCCTransaction(current.getClientId(), current.getCurrentClientCCTransaction(), current.getLastCCTransactions());
            transaction.setFraudScore(transaction.getFraudScore().add(newScore));
            store.put(key, transaction);
        }
    }
}
