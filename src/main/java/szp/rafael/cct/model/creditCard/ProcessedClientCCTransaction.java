package szp.rafael.cct.model.creditCard;

import szp.rafael.cct.collection.ConcurrentUnboundedOrderedSet;
import szp.rafael.cct.model.AbstractModel;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashSet;

public class ProcessedClientCCTransaction extends AbstractModel {

    private String clientId;
    private CreditCardTransaction currentClientCCTransaction;
    private LinkedHashSet<CreditCardTransaction> lastCCTransactions;
    private BigDecimal fraudScore = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_EVEN);

    public ProcessedClientCCTransaction() {
    }

    public ProcessedClientCCTransaction(String clientId, CreditCardTransaction currentClientCCTransaction, LinkedHashSet<CreditCardTransaction> lastCCTransactions) {
        this.clientId = clientId;
        this.currentClientCCTransaction = currentClientCCTransaction;
        this.lastCCTransactions = lastCCTransactions;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public CreditCardTransaction getCurrentClientCCTransaction() {
        return currentClientCCTransaction;
    }

    public void setCurrentClientCCTransaction(CreditCardTransaction currentClientCCTransaction) {
        this.currentClientCCTransaction = currentClientCCTransaction;
    }

    public LinkedHashSet<CreditCardTransaction> getLastCCTransactions() {
        if(lastCCTransactions==null){
            return new LinkedHashSet<>();
        }
        return lastCCTransactions;
    }

    public void setLastCCTransactions(LinkedHashSet<CreditCardTransaction> lastCCTransactions) {
        this.lastCCTransactions = lastCCTransactions;
    }

    public BigDecimal getFraudScore() {
        if(fraudScore==null){
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_EVEN);
        }
        return fraudScore;
    }

    public void setFraudScore(BigDecimal fraudScore) {
        this.fraudScore = fraudScore;
    }
}
