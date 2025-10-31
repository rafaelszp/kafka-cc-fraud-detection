package szp.rafael.cct.stream.processor;

import szp.rafael.cct.model.creditCard.CreditCardTransaction;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;
import szp.rafael.cct.utils.GeoUtils;

import java.math.BigDecimal;

public class PatternWindowCheck extends AbstractWindowProcessor {

    public static BigDecimal SMALL_AMOUNT_MAX_THRESHOLD = BigDecimal.valueOf(3.0d); // valor arbitrário
    public static BigDecimal BIG_AMOUNT_MIN_THRESHOLD = BigDecimal.valueOf(500.0d); // valor arbitrário

    public PatternWindowCheck(String storeName) {
        super(storeName);
    }

    @Override
    public EvaluationType evaluate(ProcessedClientCCTransaction transaction) {

        boolean previousIsLarge;
        boolean currentIsLarge;
        boolean previousIsSmall;
        boolean currentIsSmall;

        for (CreditCardTransaction lastTransaction : transaction.getLastCCTransactions()) {
            //verificandoo se há uma sequencia de transações pequenas seguidas de grandes
            //quando isso acontece, deve ser considerado como fraude
            previousIsLarge = lastTransaction.getAmount().compareTo(BIG_AMOUNT_MIN_THRESHOLD) > 0;
            previousIsSmall = lastTransaction.getAmount().compareTo(SMALL_AMOUNT_MAX_THRESHOLD) < 0;
            currentIsLarge = transaction.getCurrentClientCCTransaction().getAmount().compareTo(BIG_AMOUNT_MIN_THRESHOLD) > 0;
            currentIsSmall = transaction.getCurrentClientCCTransaction().getAmount().compareTo(SMALL_AMOUNT_MAX_THRESHOLD) < 0;
            if((previousIsSmall && currentIsLarge)  || (previousIsLarge && currentIsSmall)){
                return EvaluationType.FRAUD;
            }

        }
        return EvaluationType.NOT_FRAUD;
    }

    @Override
    public double getFraudScore() {
        return 0.55;
    }
}
