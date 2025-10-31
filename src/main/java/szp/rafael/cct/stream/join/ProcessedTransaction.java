package szp.rafael.cct.stream.join;

import org.slf4j.Logger;
import szp.rafael.cct.model.AbstractModel;
import szp.rafael.cct.model.AccountBalance;
import szp.rafael.cct.model.AccountTransaction;

import java.math.RoundingMode;
import java.time.Instant;

public class ProcessedTransaction extends AbstractModel {

    static Logger logger = org.slf4j.LoggerFactory.getLogger(ProcessedTransaction.class);

    private AccountTransaction transaction;
    private AccountBalance balance;

    public ProcessedTransaction(AccountTransaction transaction, AccountBalance balance) {
        this.transaction = transaction;
        this.balance = balance;
    }

    public ProcessedTransaction() {
    }

    // Lógica para aplicar a transação ao saldo atual

    public static ProcessedTransaction apply(AccountTransaction transaction, AccountBalance currentBalance) {

        logger.info("==========================================================");
        logger.info("chegou nova transação: ");
        logger.info(transaction.toJSONString());
        logger.info("Saldo Atual: ");
        logger.info(currentBalance.toJSONString());
        logger.info("==========================================================");

        double balanceAmount = currentBalance.getAmount().doubleValue();

        if (currentBalance.getLastTransactionUpdateId() > transaction.getProcessedTimestamp()) {
            AccountTransaction processedTransaction = new AccountTransaction(
                    transaction.getAccountId(),
                    transaction.getTransactionId(),
                    transaction.getTransactionType(),
                    transaction.getValue(),
                    AccountTransaction.TransactionStatus.REJECTED,
                    "Transaction ID is older than the last processed transaction."
            );
            logger.warn("Transação já processada: {}",currentBalance.getLastTransactionUpdateId());

            logger.warn("\nid atu: {}\nid env: {}",currentBalance.getLastTransactionUpdateId(),transaction.getProcessedTimestamp());
            return new ProcessedTransaction(processedTransaction, currentBalance);
        }

        if (AccountTransaction.TransactionType.DEBIT.equals(transaction.getTransactionType())) {
            AccountTransaction processedTransaction;
            if (balanceAmount < transaction.getValue().doubleValue()) {
                processedTransaction = new AccountTransaction(
                        transaction.getAccountId(),
                        transaction.getTransactionId(),
                        transaction.getTransactionType(),
                        transaction.getValue(),
                        AccountTransaction.TransactionStatus.REJECTED,
                        "Insufficient funds."
                );
                logger.warn("nao há fundos");
                return new ProcessedTransaction(processedTransaction, currentBalance);
            } else {
                balanceAmount -= transaction.getValue().doubleValue();
                processedTransaction = new AccountTransaction(
                        transaction.getAccountId(),
                        transaction.getTransactionId(),
                        transaction.getTransactionType(),
                        transaction.getValue(),
                        AccountTransaction.TransactionStatus.COMPLETED,
                        "Transaction successful."
                );
                AccountBalance newBalance = new AccountBalance(
                        transaction.getAccountId(),
                        java.math.BigDecimal.valueOf(balanceAmount).setScale(2, RoundingMode.HALF_DOWN),
                        Instant.now().toEpochMilli(),
                        transaction.getProcessedTimestamp()
                );
                logger.info("new balance: {}",newBalance.toJSONString());
                return new ProcessedTransaction(processedTransaction, newBalance);
            }
        }

        if (AccountTransaction.TransactionType.CREDIT.equals(transaction.getTransactionType())) {
            balanceAmount += transaction.getValue().doubleValue();
            AccountTransaction processedTransaction = new AccountTransaction(
                    transaction.getAccountId(),
                    transaction.getTransactionId(),
                    transaction.getTransactionType(),
                    transaction.getValue(),
                    AccountTransaction.TransactionStatus.COMPLETED,
                    "Credit successful."
            );
            AccountBalance newBalance = new AccountBalance(
                    transaction.getAccountId(),
                    java.math.BigDecimal.valueOf(balanceAmount).setScale(2, RoundingMode.HALF_DOWN),
                    Instant.now().toEpochMilli(),
                    transaction.getProcessedTimestamp()
            );
            logger.info("new balance: {}",newBalance.toJSONString());
            return new ProcessedTransaction(processedTransaction, newBalance);
        }

        AccountTransaction processedTransaction = new AccountTransaction(
                transaction.getAccountId(),
                transaction.getTransactionId(),
                transaction.getTransactionType(),
                transaction.getValue(),
                AccountTransaction.TransactionStatus.FAILED,
                "Failed to process transaction because no situation was matched."
        );
        logger.warn("deu merda");
        return new ProcessedTransaction(processedTransaction, currentBalance);

    }

    public AccountTransaction getTransaction() {
        return transaction;
    }

    public void setTransaction(AccountTransaction transaction) {
        this.transaction = transaction;
    }

    public AccountBalance getBalance() {
        return balance;
    }

    public void setBalance(AccountBalance balance) {
        this.balance = balance;
    }
}
