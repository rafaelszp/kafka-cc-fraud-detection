package szp.rafael.cct.collection.adapters;

import com.google.gson.*;
import szp.rafael.cct.collection.ConcurrentUnboundedOrderedSet;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Gson adapter that serializes ConcurrentUnboundedOrderedSet<E> as a JSON array (ordered list)
 * and deserializes it back, attempting to preserve element typing using the provided Type.
 *
 * When registering, prefer registering with a TypeToken that includes element type, e.g.:
 *   Type type = new TypeToken<ConcurrentUnboundedOrderedSet<String>>(){}.getType();
 *   gsonBuilder.registerTypeAdapter(type, new ConcurrentUnboundedOrderedSetAdapter<>());
 *
 * If element type cannot be determined, elements will be deserialized as generic JsonElement -> Object.
 */
@SuppressWarnings("unchecked")
public class ConcurrentUnboundedOrderedSetAdapter<E> implements JsonSerializer<ConcurrentUnboundedOrderedSet<E>>,
        JsonDeserializer<ConcurrentUnboundedOrderedSet<E>> {

    @Override
    public JsonElement serialize(ConcurrentUnboundedOrderedSet<E> src, Type typeOfSrc, JsonSerializationContext context) {
        // attempt to get element type to serialize list with correct type; otherwise fallback
        Type elemType = extractElementType(typeOfSrc);
        List<E> list = src.toList();
        if (elemType != null) {
            // serialize list with element type hint
            Type listType = com.google.gson.reflect.TypeToken.getParameterized(List.class, elemType).getType();
            return context.serialize(list, listType);
        } else {
            return context.serialize(list);
        }
    }

    @Override
    public ConcurrentUnboundedOrderedSet<E> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        ConcurrentUnboundedOrderedSet<E> set = new ConcurrentUnboundedOrderedSet<>();
        if (!json.isJsonArray()) {
            throw new JsonParseException("Expected JSON array for ConcurrentUnboundedOrderedSet");
        }
        JsonArray arr = json.getAsJsonArray();
        Type elemType = extractElementType(typeOfT);
        for (JsonElement je : arr) {
            E value;
            if (elemType != null) {
                value = context.deserialize(je, elemType);
            } else {
                // best effort: deserialize to Object (Gson default handling)
                value = (E) context.deserialize(je, Object.class);
            }
            set.add(value);
        }
        return set;
    }

    /**
     * If type is ConcurrentUnboundedOrderedSet<T>, returns T. Otherwise returns null.
     */
    private Type extractElementType(Type typeOfSrc) {
        if (typeOfSrc instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) typeOfSrc;
            Type[] args = pt.getActualTypeArguments();
            if (args != null && args.length == 1) {
                return args[0];
            }
        }
        return null;
    }
}

