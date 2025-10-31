package szp.rafael.cct.serde;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.serialization.Serializer;
import szp.rafael.cct.collection.ConcurrentUnboundedOrderedSet;
import szp.rafael.cct.collection.adapters.ConcurrentUnboundedOrderedSetAdapter;
import szp.rafael.cct.model.creditCard.ProcessedClientCCTransaction;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

    private Gson gson = gsonBuilder();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) return null;
        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
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
