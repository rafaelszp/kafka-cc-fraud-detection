package szp.rafael.cct.serde;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.serialization.Deserializer;
import szp.rafael.cct.collection.ConcurrentUnboundedOrderedSet;
import szp.rafael.cct.collection.adapters.ConcurrentUnboundedOrderedSetAdapter;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;

import java.lang.reflect.Type;
import java.util.Map;

public class JSONDeserializer<T> implements Deserializer<T> {

    private Class<T> destinationClass;
    private Type reflectionTypeToken;
    private Gson gson =
            gsonBuilder();


    public JSONDeserializer(Class<T> tClass) {
        this.destinationClass = tClass;
    }

    public JSONDeserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        Type type = destinationClass != null ? destinationClass : reflectionTypeToken;
        return gson.fromJson(new String(bytes), type);
    }


    private static Gson gsonBuilder() {
        Type processedClientCreditCardTransactionType = new TypeToken<ConcurrentUnboundedOrderedSet<ProcessedClientCCTransaction>>() {}.getType();
        Gson gson = gsonFor(ProcessedClientCCTransaction.class, processedClientCreditCardTransactionType);
        return gson;
    }

    private static <T> Gson gsonFor(Class<T> clazz, Type typeToken) {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(typeToken, new ConcurrentUnboundedOrderedSetAdapter<>());
        builder.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);
        return builder.create();
    }
}

