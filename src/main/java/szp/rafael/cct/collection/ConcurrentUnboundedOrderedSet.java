package szp.rafael.cct.collection;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * ConcurrentUnboundedOrderedSet: coleção ordenada por inserção (mais antigos no início),
 * sem duplicatas, alto desempenho concorrente (estratégia sem locks globais).
 *
 * Representa a ordem de inserção: itens mais recentes ficam no final (tail).
 *
 * Nota: elementos usados devem ter equals/hashCode estáveis enquanto estiverem na coleção.
 *
 * @param <E> tipo do elemento
 */
public class ConcurrentUnboundedOrderedSet<E> implements Iterable<E> {

    private final ConcurrentHashMap<E, Entry<E>> map = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<Entry<E>> deque = new ConcurrentLinkedDeque<>();

    private static final class Entry<E> {
        final E value;
        final AtomicBoolean removed = new AtomicBoolean(false);

        Entry(E value) { this.value = Objects.requireNonNull(value); }
    }

    public ConcurrentUnboundedOrderedSet() { }

    /**
     * Constrói a partir de uma coleção (mantém a ordem de iteração da coleção).
     */
    public ConcurrentUnboundedOrderedSet(Collection<? extends E> elements) {
        if (elements != null) {
            for (E e : elements) add(e);
        }
    }

    // ---------------- basic ops ----------------

    /**
     * Adiciona elemento no final se não existir.
     *
     * @return true se adicionado (não existia), false se já existia.
     */
    public boolean add(E element) {
        Objects.requireNonNull(element);
        Entry<E> newEntry = new Entry<>(element);
        Entry<E> prev = map.putIfAbsent(element, newEntry);
        if (prev == null) {
            deque.addLast(newEntry);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Adiciona o elemento no final. Se já existir, move-o para o final.
     *
     * @return true se elemento foi adicionado novo, false se apenas movido para o final.
     */
    public boolean addOrMoveToLast(E element) {
        Objects.requireNonNull(element);
        while (true) {
            Entry<E> cur = map.get(element);
            if (cur == null) {
                Entry<E> newEntry = new Entry<>(element);
                Entry<E> prev = map.putIfAbsent(element, newEntry);
                if (prev == null) {
                    deque.addLast(newEntry);
                    return true;
                } else {
                    // alguém inseriu concorrentemente -> retry
                    continue;
                }
            } else {
                Entry<E> newEntry = new Entry<>(element);
                boolean replaced = map.replace(element, cur, newEntry);
                if (replaced) {
                    cur.removed.set(true);
                    deque.addLast(newEntry);
                    return false;
                } else {
                    // map changed concurrently; retry
                    continue;
                }
            }
        }
    }

    /**
     * Remove e retorna o elemento mais antigo (topo). Retorna null se vazio.
     * Faz limpeza de entradas marcadas como removed.
     */
    public E removeOldest() {
        while (true) {
            Entry<E> e = deque.pollFirst();
            if (e == null) return null; // vazio
            if (e.removed.get()) {
                // entrada antiga/invalida -> ignore
                continue;
            }
            // tenta remover do map somente se este Entry ainda for a entry atual
            boolean removedFromMap = map.remove(e.value, e);
            if (removedFromMap) {
                e.removed.set(true);
                return e.value;
            } else {
                // provavelmente foi movido para o final -> ignore
                continue;
            }
        }
    }

    /**
     * Retorna (sem remover) o elemento mais antigo. Retorna null se vazio.
     */
    public E peekOldest() {
        for (Entry<E> e : deque) {
            if (e.removed.get()) continue;
            Entry<E> mapping = map.get(e.value);
            if (mapping == e) return e.value;
        }
        return null;
    }

    /**
     * Remove um elemento específico.
     *
     * @return true se existia e foi removido.
     */
    public boolean remove(E element) {
        Objects.requireNonNull(element);
        Entry<E> e = map.remove(element);
        if (e != null) {
            e.removed.set(true);
            return true;
        }
        return false;
    }

    /**
     * Retorna se contém o elemento (com base no map).
     */
    public boolean contains(E element) {
        Objects.requireNonNull(element);
        return map.containsKey(element);
    }

    /**
     * Retorna número de elementos (tamanho atual).
     */
    public int size() {
        return map.size();
    }

    /**
     * Limpa a coleção imediatamente.
     */
    public void clear() {
        for (Entry<E> e : map.values()) {
            e.removed.set(true);
        }
        map.clear();
        deque.clear();
    }

    /**
     * Snapshot dos elementos ordenados (do mais antigo ao mais recente).
     */
    public List<E> toList() {
        List<E> result = new ArrayList<>();
        for (Entry<E> e : deque) {
            if (e.removed.get()) continue;
            Entry<E> mapping = map.get(e.value);
            if (mapping == e) result.add(e.value);
        }
        return result;
    }

    @Override
    public Iterator<E> iterator() {
        // devolve iterator sobre snapshot para segurança
        return Collections.unmodifiableList(toList()).iterator();
    }

    // ---------------- advanced ops ----------------

    /**
     * Remove todos os elementos que satisfazem o predicado.
     *
     * @return true se ao menos um elemento foi removido.
     */
    public boolean removeIf(Predicate<? super E> filter) {
        Objects.requireNonNull(filter);
        boolean removedAny = false;
        for (Map.Entry<E, Entry<E>> me : map.entrySet()) {
            E key = me.getKey();
            Entry<E> entry = me.getValue();
            if (filter.test(key)) {
                boolean removed = map.remove(key, entry);
                if (removed) {
                    entry.removed.set(true);
                    removedAny = true;
                }
            }
        }
        return removedAny;
    }

    /**
     * Retorna snapshot filtrada (do mais antigo ao mais recente).
     */
    public List<E> filter(Predicate<? super E> predicate) {
        Objects.requireNonNull(predicate);
        return toList().stream().filter(predicate).collect(Collectors.toList());
    }

    /**
     * Mantém apenas os últimos 'num' elementos (os mais recentes).
     * Remove os mais antigos até que size() <= num.
     */
    public void keepOnlyLast(int num) {
        if (num < 0) throw new IllegalArgumentException("num must be >= 0");
        while (size() > num) {
            E removed = removeOldest();
            if (removed == null) break;
        }
    }
}
