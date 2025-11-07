package szp.rafael.cct.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JSONSerdes<T> implements Serde<T> {

    private Class<T> tClass;

    public JSONSerdes(Class<T> tClass) {
        this.tClass = tClass;
    }


    @Override
    public Serializer<T> serializer() {
                    return new JsonSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new JSONDeserializer<>(tClass);
    }
}
