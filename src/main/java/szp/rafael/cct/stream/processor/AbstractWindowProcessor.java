package szp.rafael.cct.stream.processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import szp.rafael.cct.model.creditCard.CreditCardTransaction;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;

public abstract class AbstractWindowProcessor implements ProcessorSupplier<String, CreditCardTransaction,String, ProcessedClientCCTransaction> {

    public abstract EvaluationType evaluate(ProcessedClientCCTransaction transaction);
    public abstract double getFraudScore();

    public String storeName;
    Logger logger;
    private final Duration windowSize = Duration.ofMinutes(30);


    public AbstractWindowProcessor(String storeName) {
        this.storeName = storeName;
        logger = getLogger();
    }

    public Logger getLogger() {
        if (logger == null) {
            logger = org.slf4j.LoggerFactory.getLogger(this.getClass());
        }
        return logger;
    }


    @Override
    public Processor<String, CreditCardTransaction, String, ProcessedClientCCTransaction> get() {

        return new Processor<String, CreditCardTransaction, String, ProcessedClientCCTransaction>() {

            private ProcessorContext<String, ProcessedClientCCTransaction> context;
            private WindowStore<String, ProcessedClientCCTransaction> transactionStore;

            @Override
            public void init(ProcessorContext<String, ProcessedClientCCTransaction> context) {
                transactionStore = context.getStateStore(storeName);
                this.context = context;
            }

            @Override
            public void process(Record<String, CreditCardTransaction> record) {

                String clientId = record.key();
                CreditCardTransaction clientTransaction = record.value();
                Instant windowStart = Instant.ofEpochMilli(record.timestamp()).minus(windowSize);

                ProcessedClientCCTransaction transaction = new ProcessedClientCCTransaction(record.value().getClientId(), record.value(), new LinkedHashSet<>());

                //Aqui estou buscando as transações 30min antes da transação atual
                //Será registrado na store todas as transações dentro do range de 30min
                try(WindowStoreIterator<ProcessedClientCCTransaction> iterator = transactionStore.fetch(clientId, windowStart.toEpochMilli(),clientTransaction.getTimestamp())){
                    logger.debug("loop --------------------------------------------------------------->\n");
                    while (iterator.hasNext()) {
                        KeyValue<Long, ProcessedClientCCTransaction> next = iterator.next();
                        logger.debug("\n\ncurrent {}",clientTransaction.toJSONString());
                        logger.debug("fetch   {}\n\n",next.value.toJSONString());
                        transaction.getLastCCTransactions().add(next.value.getCurrentClientCCTransaction());
                    }
                }

                EvaluationType evaluation = evaluate(transaction);
                if (evaluation == EvaluationType.FRAUD) {
                    transaction.setFraudScore(transaction.getFraudScore().add(BigDecimal.valueOf(getFraudScore())).setScale(3, RoundingMode.HALF_EVEN));
                }
                Record<String, ProcessedClientCCTransaction> fwd = new Record<String, ProcessedClientCCTransaction>(record.key(),transaction, record.timestamp());

                transactionStore.put(record.key(), transaction,clientTransaction.getTimestamp());

                context.forward(fwd);
            }
        };
    }


    public enum EvaluationType {
        FRAUD,
        NOT_FRAUD
    }

}
