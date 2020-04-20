package com.devebot.opflow.supports;

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author acegik
 */
public class OpflowCyclingList<V> {
    private final List<V> store = new LinkedList<>();
    private int pos = 0;
    
    public void add(V item) {
        store.add(item);
    }
    
    public V next() {
        if (pos >= store.size()) {
            pos = 0;
        }
        return store.get(pos++);
    }
}
