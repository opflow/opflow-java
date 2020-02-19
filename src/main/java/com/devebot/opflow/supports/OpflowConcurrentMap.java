package com.devebot.opflow.supports;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author drupalex
 * 
 * @param <K>
 * @param <V>
 */
public class OpflowConcurrentMap<K, V> extends ConcurrentHashMap<K, V> {

    private volatile int maxSize = 0;

    public OpflowConcurrentMap() {
        super();
    }

    public OpflowConcurrentMap(int initialCapacity) {
        super(initialCapacity);
    }

    public OpflowConcurrentMap(Map<? extends K, ? extends V> m) {
        super(m);
    }

    public OpflowConcurrentMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public OpflowConcurrentMap(int initialCapacity, float loadFactor, int concurrencyLevel) {
        super(initialCapacity, loadFactor, concurrencyLevel);
    }

    public int getMaxSize() {
        return maxSize;
    }
    
    @Override
    public V put(K key, V value) {
        V v = super.put(key, value);
        int len = this.size();
        if (len > maxSize) {
            maxSize = len;
        }
        return v;
    }
}
