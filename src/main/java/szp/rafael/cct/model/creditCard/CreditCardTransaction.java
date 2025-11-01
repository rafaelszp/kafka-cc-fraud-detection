package szp.rafael.cct.model.creditCard;

import szp.rafael.cct.model.AbstractModel;

import java.math.BigDecimal;

public class CreditCardTransaction extends AbstractModel {

    private String transactionId;
    private String clientId; // ID do cliente (Chave do tópico Kafka)
    private BigDecimal amount;   // Valor da transação
    private long timestamp; // Timestamp UTC
    private boolean isFraud;

    // Objetos aninhados
    private CardDetails cardDetails;
    private Geolocation geolocation;
    private IpData ipData;

    // Construtor padrão (necessário para Kafka Streams)
    public CreditCardTransaction() {
    }

    // Construtor completo
    public CreditCardTransaction(String transactionId, String clientId, BigDecimal amount, long timestamp,
                                 CardDetails cardDetails, Geolocation geolocation, IpData ipData) {
        this.transactionId = transactionId;
        this.clientId = clientId;
        this.amount = amount;
        this.timestamp = timestamp;
        this.cardDetails = cardDetails;
        this.geolocation = geolocation;
        this.ipData = ipData;
    }

    // Getters e Setters
    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public CardDetails getCardDetails() {
        return cardDetails;
    }

    public void setCardDetails(CardDetails cardDetails) {
        this.cardDetails = cardDetails;
    }

    public Geolocation getGeolocation() {
        return geolocation;
    }

    public void setGeolocation(Geolocation geolocation) {
        this.geolocation = geolocation;
    }

    public IpData getIpData() {
        return ipData;
    }

    public void setIpData(IpData ipData) {
        this.ipData = ipData;
    }

    public boolean isFraud() {
        return isFraud;
    }

    public void setFraud(boolean fraud) {
        isFraud = fraud;
    }
}
