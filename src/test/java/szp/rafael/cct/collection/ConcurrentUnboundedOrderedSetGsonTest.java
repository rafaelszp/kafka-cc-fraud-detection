package szp.rafael.cct.collection;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.junit.jupiter.api.Test;
import szp.rafael.cct.collection.adapters.ConcurrentUnboundedOrderedSetAdapter;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Testes JUnit 5 para ConcurrentUnboundedOrderedSet + Gson adapter.
 */
public class ConcurrentUnboundedOrderedSetGsonTest {

    private <T> Gson gsonFor(Class<T> clazz, Type typeToken) {
        GsonBuilder builder = new GsonBuilder();
        // register adapter for the specific parametrized type (recommended)
        builder.registerTypeAdapter(typeToken, new ConcurrentUnboundedOrderedSetAdapter<>());
        return builder.create();
    }

    @Test
    void testAddAndSerializeDeserializeString() {
        Type setType = new TypeToken<ConcurrentUnboundedOrderedSet<String>>() {}.getType();
        Gson gson = gsonFor(String.class, setType);

        ConcurrentUnboundedOrderedSet<String> set = new ConcurrentUnboundedOrderedSet<>();
        set.add("A");
        set.add("B");
        set.add("C");

        String json = gson.toJson(set, setType);
        assertEquals("[\"A\",\"B\",\"C\"]", json);

        ConcurrentUnboundedOrderedSet<String> fromJson = gson.fromJson(json, setType);
        assertNotNull(fromJson);
        assertEquals(3, fromJson.size());
        assertEquals(Arrays.asList("A", "B", "C"), fromJson.toList());
    }

    @Test
    void testKeepOnlyLastAndFilterRemoveIf() {
        ConcurrentUnboundedOrderedSet<String> set = new ConcurrentUnboundedOrderedSet<>();
        set.add("A"); set.add("B"); set.add("C"); set.add("D"); set.add("E");
        assertEquals(Arrays.asList("A","B","C","D","E"), set.toList());

        set.keepOnlyLast(3);
        assertEquals(Arrays.asList("C","D","E"), set.toList());

        // move D to last
        set.addOrMoveToLast("D");
        assertEquals(Arrays.asList("C","E","D"), set.toList());

        // remove items starting with 'D'
        boolean removed = set.removeIf(s -> s.startsWith("D"));
        assertTrue(removed);
        assertEquals(Arrays.asList("C","E"), set.toList());

        List<String> filtered = set.filter(s -> s.contains("E"));
        assertEquals(Collections.singletonList("E"), filtered);
    }

    @Test
    void testSerializationPreservesOrderForComplexType() {
        Type setType = new TypeToken<ConcurrentUnboundedOrderedSet<Map<String,Integer>>>() {}.getType();
        Gson gson = gsonFor(Map.class, setType);

        ConcurrentUnboundedOrderedSet<Map<String,Integer>> set = new ConcurrentUnboundedOrderedSet<>();
        Map<String,Integer> a = Collections.singletonMap("x", 1);
        Map<String,Integer> b = Collections.singletonMap("y", 2);
        set.add(a); set.add(b);

        String json = gson.toJson(set, setType);
        // json should be array of objects; basic check:
        assertTrue(json.startsWith("["));
        assertTrue(json.contains("\"x\":1") || json.contains("\"y\":2"));

        ConcurrentUnboundedOrderedSet<Map<String,Integer>> fromJson = gson.fromJson(json, setType);
        assertEquals(2, fromJson.size());
        assertEquals(set.toList(), fromJson.toList());
    }

    @Test
    void testConcurrentAddAndMove() throws InterruptedException, ExecutionException {
        ConcurrentUnboundedOrderedSet<Integer> set = new ConcurrentUnboundedOrderedSet<>();
        int threads = 8;
        int perThread = 1000;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            tasks.add(() -> {
                start.await();
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                for (int i = 0; i < perThread; i++) {
                    int value = rnd.nextInt(0, perThread); // many collisions
                    if (rnd.nextBoolean()) {
                        set.add(value);
                    } else {
                        set.addOrMoveToLast(value);
                    }
                }
                return null;
            });
        }

        List<Future<Void>> futures = new ArrayList<>();
        for (Callable<Void> c : tasks) {
            futures.add(exec.submit(c));
        }
        start.countDown(); // go
        for (Future<Void> f : futures) f.get();
        exec.shutdown();
        exec.awaitTermination(5, TimeUnit.SECONDS);

        // verify uniqueness
        List<Integer> snapshot = set.toList();
        Set<Integer> unique = new HashSet<>(snapshot);
        assertEquals(unique.size(), snapshot.size());

        // size should be <= perThread
        assertTrue(set.size() <= perThread);

        // remove even numbers
        boolean removed = set.removeIf(x -> x % 2 == 0);
        // removed may be true or false depending on contents, but operation should not throw
        assertNotNull(removed);

        set.clear();
        assertEquals(0, set.size());
    }
}
