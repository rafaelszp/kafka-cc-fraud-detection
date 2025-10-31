package szp.rafael.cct.stream.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import szp.rafael.cct.model.AccountBalance;
import szp.rafael.cct.model.AccountTransaction;
import szp.rafael.cct.stream.join.ProcessedTransaction;

import java.math.BigDecimal;


public class TransactionProcessor implements ProcessorSupplier<String, AccountTransaction,String, ProcessedTransaction> {

    static Logger logger = org.slf4j.LoggerFactory.getLogger(TransactionProcessor.class);

    private String storeName;


    public TransactionProcessor(String accountBalanceStore) {
        this.storeName = accountBalanceStore;
    }

    @Override
    public Processor<String, AccountTransaction, String, ProcessedTransaction> get() {
        return new Processor<String, AccountTransaction, String, ProcessedTransaction>() {

            private ProcessorContext<String, ProcessedTransaction> context;
            private KeyValueStore<String, AccountBalance> balanceStore;

            @Override
            public void init(ProcessorContext<String, ProcessedTransaction> context) {
                balanceStore = context.getStateStore(storeName);
                this.context = context;
            }

            @Override
            public void process(Record<String, AccountTransaction> record) {
                String accountId = record.key();
                AccountTransaction transaction = record.value();

                AccountBalance currentBalance = getCurrentBalance(accountId);

                ProcessedTransaction processedTransaction = ProcessedTransaction.apply(transaction, currentBalance);
                logger.info("new balance to store: {}",processedTransaction.getBalance().toJSONString());
                balanceStore.put(accountId, processedTransaction.getBalance());

                context.forward(new Record<>(accountId, processedTransaction, record.timestamp()));
            }

            private AccountBalance getCurrentBalance(String accountId) {
                AccountBalance balance = balanceStore.get(accountId);
                if(balance==null){
                    logger.info("Saldo nao encontrado para conta: {}",accountId);
                    balance = new AccountBalance(accountId, BigDecimal.ZERO,0L,0L);
                    balanceStore.put(accountId,balance);
                }
                return balance;
            }
        };
    }
}
