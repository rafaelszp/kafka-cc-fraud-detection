package szp.rafael.cct.stream.processor;

import io.vavr.control.Try;
import szp.rafael.cct.model.creditCard.CreditCardTransaction;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;
import szp.rafael.cct.utils.IpUtils;

import java.net.InetAddress;

public class MultipleIPWindowCheck extends AbstractWindowProcessor {

    public static final int MAX_DIFFERENT_IP_ADDRESSESES = 4;

    public MultipleIPWindowCheck(String storeName) {
        super(storeName);
    }

    @Override
    public EvaluationType evaluate(ProcessedClientCCTransaction transaction) {
        if(transaction.getLastCCTransactions().isEmpty()){
            return EvaluationType.NOT_FRAUD;
        }
        int mixCount = 0;
        for (CreditCardTransaction lastTransaction : transaction.getLastCCTransactions()) {
            InetAddress ip = Try.of(() -> IpUtils.parseInetAddress(lastTransaction.getIpData().getPublicIpAddress())).getOrElse(InetAddress.getLoopbackAddress());
            if(!IpUtils.isPublic(ip)){
                return EvaluationType.FRAUD;
            }
            if(!IpUtils.areSamePublicIP(lastTransaction.getIpData().getPublicIpAddress(),transaction.getCurrentClientCCTransaction().getIpData().getPublicIpAddress())){
                mixCount++;
            }
        }
        if(mixCount> MAX_DIFFERENT_IP_ADDRESSESES){ //valor arbitrário
            return EvaluationType.FRAUD;
        }
        return EvaluationType.NOT_FRAUD;

    }

    @Override
    public double getFraudScore() {
        return 0.75; //valor abritrário
    }
}
