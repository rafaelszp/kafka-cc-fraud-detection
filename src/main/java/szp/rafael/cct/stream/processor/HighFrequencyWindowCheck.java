package szp.rafael.cct.stream.processor;

import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;

import java.time.Duration;

public class HighFrequencyWindowCheck extends AbstractWindowProcessor{

    public static final int MAX_TRANSACTIONS_IN_WINDOW = 10;

    public HighFrequencyWindowCheck(String storeName) {
        super(storeName);
    }

    @Override
    public EvaluationType evaluate(ProcessedClientCCTransaction transaction) {

        if(transaction.getLastCCTransactions().size()>=MAX_TRANSACTIONS_IN_WINDOW){
            return EvaluationType.FRAUD;
        }

        return EvaluationType.NOT_FRAUD;
    }

    @Override
    public double getFraudScore() {
        return 1.0;
    }
}
