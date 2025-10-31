package szp.rafael.cct.stream.processor;

import szp.rafael.cct.model.creditCard.CreditCardTransaction;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;
import szp.rafael.cct.utils.GeoUtils;

public class GeoWindowCheck extends AbstractWindowProcessor {

    public GeoWindowCheck(String storeName) {
        super(storeName);
    }

    @Override
    public EvaluationType evaluate(ProcessedClientCCTransaction transaction) {
       if(transaction.getLastCCTransactions().isEmpty()){
           return EvaluationType.NOT_FRAUD;
       }

       double sumDistance = 0.0d;
        for (CreditCardTransaction lastTransaction : transaction.getLastCCTransactions()) {
            double distance = GeoUtils.calcularDistancia(transaction.getCurrentClientCCTransaction().getGeolocation().getLatitude(), transaction.getCurrentClientCCTransaction().getGeolocation().getLongitude(), lastTransaction.getGeolocation().getLatitude(), lastTransaction.getGeolocation().getLongitude());
            sumDistance += distance;
        }

        if(sumDistance>30.0d){
            return EvaluationType.FRAUD;
        }

        return EvaluationType.NOT_FRAUD;
    }

    @Override
    public double getFraudScore() {
        return 0.9d; //valor arbitrário
    }
}
