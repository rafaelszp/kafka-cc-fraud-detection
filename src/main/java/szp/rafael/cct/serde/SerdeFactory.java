package szp.rafael.cct.serde;

public class SerdeFactory<T> {

    public JSONSerdes<T> createSerde(Class<T> clazz){
        return new JSONSerdes<>(clazz);
    }

}

