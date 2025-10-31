package szp.rafael.cct.model;

import java.math.BigDecimal;
import java.time.Instant;

public class AccountTransaction extends AbstractModel  {

    private String accountId;
    private String transactionId;
    private TransactionType transactionType;
    private BigDecimal value;
    private TransactionStatus status;
    private String reason;
    private Long processedTimestamp;


    public AccountTransaction(String accountId, String transactionId, TransactionType transactionType, BigDecimal value) {
        this.accountId = accountId;
        this.transactionId = transactionId;
        this.transactionType = transactionType;
        this.value = value;
        this.status = TransactionStatus.PENDING;
        this.reason = "PENDING";
        this.processedTimestamp = Instant.now().toEpochMilli();
    }

    public AccountTransaction(String accountId, String transactionId, TransactionType transactionType, BigDecimal value, TransactionStatus status, String reason) {
        this.accountId = accountId;
        this.transactionId = transactionId;
        this.transactionType = transactionType;
        this.value = value;
        this.status = status;
        this.reason = reason;
        this.processedTimestamp = Instant.now().toEpochMilli();
    }

    public AccountTransaction() {
    }

    public enum TransactionType{
        CREDIT,
        DEBIT
    }

    public enum TransactionStatus{
        PENDING,
        COMPLETED,
        FAILED,
        REJECTED
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public Long getProcessedTimestamp() {
        return processedTimestamp;
    }

    public void setProcessedTimestamp(Long processedTimestamp) {
        this.processedTimestamp = processedTimestamp;
    }
}
