package szp.rafael.cct.stream.join;

import org.apache.kafka.streams.kstream.ValueJoiner;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;

import java.util.LinkedHashSet;

public class ProcessedClientCCJoiner implements ValueJoiner<ProcessedClientCCTransaction, ProcessedClientCCTransaction,ProcessedClientCCTransaction> {

    @Override
    public ProcessedClientCCTransaction apply(ProcessedClientCCTransaction left, ProcessedClientCCTransaction right) {
        ProcessedClientCCTransaction joined = new ProcessedClientCCTransaction(left.getClientId(),left.getCurrentClientCCTransaction(),new LinkedHashSet<>());
        joined.setFraudScore(left.getFraudScore().add(right.getFraudScore()));
        return joined;
    }
}
