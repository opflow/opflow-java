package com.devebot.opflow.supports;

import java.util.HashMap;

/**
 *
 * @author acegik
 */
public class OpflowRevolvingMap<K, V> {
    public interface ChangeListener<K, V> {
        default void onCreated(K key, V object) {}
        default void onUpdated(K key, V oldObject, V newObject) {}
        default void onDeleted(K key, V object) {}
    }

    private final ChangeListener<K, V> changeListener;
    private final Object changeLock = new Object();
    private final HashMap<K, OpflowCircularList.Node<V>> lookupTable = new HashMap<>();
    private final OpflowCircularList<V> revolver = new OpflowCircularList<>();
    
    public OpflowRevolvingMap() {
        this(null);
    }

    public OpflowRevolvingMap(ChangeListener<K, V> changeListener) {
        this.changeListener = changeListener;
    }
    
    public V get(K key) {
        if (lookupTable.containsKey(key)) {
            OpflowCircularList.Node<V> node = lookupTable.get(key);
            if (node != null) {
                return node.getRef();
            }
        }
        return null;
    }
    
    public void put(K key, V obj) {
        synchronized (changeLock) {
            if (lookupTable.containsKey(key)) {
                OpflowCircularList.Node<V> node = lookupTable.get(key);
                V oldObj = node.getRef();
                node.setRef(obj);
                if (changeListener != null) {
                    changeListener.onUpdated(key, oldObj, obj);
                }
            } else {
                OpflowCircularList.Node<V> node = revolver.createNode(obj);
                revolver.appendNode(node);
                lookupTable.put(key, node);
                if (changeListener != null) {
                    changeListener.onCreated(key, obj);
                }
            }
        }
    }
    
    public V remove(K key) {
        synchronized (changeLock) {
            if (lookupTable.containsKey(key)) {
                OpflowCircularList.Node<V> node = lookupTable.remove(key);
                revolver.removeNode(node);
                if (changeListener != null) {
                    changeListener.onDeleted(key, node.getRef());
                }
                return node.getRef();
            }
        }
        return null;
    }
    
    public V rotate() {
        return revolver.nextRef();
    }
}
