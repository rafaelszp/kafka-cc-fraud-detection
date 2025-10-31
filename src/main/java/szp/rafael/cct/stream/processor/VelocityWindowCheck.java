package szp.rafael.cct.stream.processor;

import szp.rafael.cct.model.creditCard.CreditCardTransaction;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;

import java.math.BigDecimal;
import java.time.Duration;

public class VelocityWindowCheck extends AbstractWindowProcessor{

    public static final BigDecimal MAX_AMOUNT_PER_MINUTE_THRESHOLD = BigDecimal.valueOf(1000.0d);

    public VelocityWindowCheck(String storeName) {
        super(storeName);
    }

    @Override
    public EvaluationType evaluate(ProcessedClientCCTransaction transaction) {

        if(transaction.getLastCCTransactions().isEmpty()){
            return EvaluationType.NOT_FRAUD;
        }

        double sumAmount = 0.0d;

        CreditCardTransaction firstTransaction = transaction.getLastCCTransactions().stream().findFirst().get();
        CreditCardTransaction lastTransaction = transaction.getLastCCTransactions().stream().reduce((first, second) -> second).get();
        for (CreditCardTransaction fetchTransaction : transaction.getLastCCTransactions()) {
            sumAmount += fetchTransaction.getAmount().doubleValue();
        }

        if(exceedsRate(sumAmount,firstTransaction.getTimestamp(),lastTransaction.getTimestamp())){
            return EvaluationType.FRAUD;
        }

        return EvaluationType.NOT_FRAUD;
    }

    @Override
    public double getFraudScore() {
        return 0.99;
    }

    /**
     * Verifica se a taxa média de transações (R$/minuto)
     * entre dois timestamps excede o limite especificado.
     *
     * @param totalAmount valor total das transações no intervalo
     * @param firstTimestampMs timestamp (epoch ms) do primeiro evento
     * @param lastTimestampMs timestamp (epoch ms) do último evento
     * @return true se a taxa média exceder o limite
     */
    private static boolean exceedsRate(double totalAmount,
                                      long firstTimestampMs,
                                      long lastTimestampMs) {
        if (lastTimestampMs <= firstTimestampMs) {
            // evita divisão por zero, considera mínimo de 1 segundo
            lastTimestampMs = firstTimestampMs + 1000;
        }

        Duration duration = Duration.ofMillis(lastTimestampMs - firstTimestampMs);
        double minutes = duration.toMillis() / 60000.0; // converte para minutos

        if (minutes <= 0.0) {
            // fallback de segurança
            minutes = 1.0 / 60.0; // 1 segundo
        }

        double rate = totalAmount / minutes;
        return rate > MAX_AMOUNT_PER_MINUTE_THRESHOLD.doubleValue();
    }

    private static double computeRate(double totalAmount,
                                     long firstTimestampMs,
                                     long lastTimestampMs) {
        if (lastTimestampMs <= firstTimestampMs) {
            return totalAmount; // duração nula: taxa = total instantânea
        }
        Duration duration = Duration.ofMillis(lastTimestampMs - firstTimestampMs);
        double minutes = duration.toMillis() / 60000.0;
        return totalAmount / minutes;
    }
}
