package szp.rafael.cct.model;

import com.google.gson.annotations.SerializedName;

import java.math.BigDecimal;

public class AccountBalance extends AbstractModel  {

    private String accountId;

    @SerializedName("current_balance")
    private BigDecimal amount;

    private Long lastUpdateTimestamp;

    private Long lastTransactionUpdateId;


    public AccountBalance(String accountId, BigDecimal amount, Long lastUpdateTimestamp, Long lastTransactionUpdateId) {
        this.accountId = accountId;
        this.amount = amount;
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.lastTransactionUpdateId = lastTransactionUpdateId;
    }

    public AccountBalance() {
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public Long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(Long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public Long getLastTransactionUpdateId() {
        return lastTransactionUpdateId;
    }

    public void setLastTransactionUpdateId(Long lastTransactionUpdateId) {
        this.lastTransactionUpdateId = lastTransactionUpdateId;
    }
}
