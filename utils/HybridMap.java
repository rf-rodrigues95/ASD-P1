package utils;

import java.util.*;

public class HybridMap<K extends Comparable<K>, V> {
    private final LinkedHashMap<K, V> insertionOrderMap = new LinkedHashMap<>();
    private final NavigableMap<K, V> sortedMap = new TreeMap<>();
    private final Random random = new Random();

    public void put(K key, V value) {
        insertionOrderMap.put(key, value);
        sortedMap.put(key, value);
    }

    public void putIfAbsent(K key, V value) {
        insertionOrderMap.putIfAbsent(key, value);
        sortedMap.putIfAbsent(key, value);
    }

    public V get(K key) {
        return sortedMap.get(key);
    }

    public void remove(K key) {
        insertionOrderMap.remove(key);
        sortedMap.remove(key);
    }

    public int size() {
        return sortedMap.size();
    }

    public Set<K> descendingKeySet() {
        final List<K> keys = new ArrayList<>(insertionOrderMap.keySet());
        Collections.reverse(keys);
        return new LinkedHashSet<>(keys);
    }

    public Map.Entry<K, V> first() {
        return insertionOrderMap.entrySet().iterator().next();
    }

    @Override
    public String toString() {
        return "HybridMap{" +
               "insertionOrderMap=" + insertionOrderMap +
               ", sortedMap=" + sortedMap +
               ", random=" + random +
               '}';
    }
}