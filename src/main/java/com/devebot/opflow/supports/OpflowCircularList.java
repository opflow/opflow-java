package com.devebot.opflow.supports;

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author acegik
 */
public class OpflowCircularList<V> {
    
    private Node<V> current = null;

    public Node<V> createNode(V ref) {
        return new Node(null, ref, null);
    }
    
    public void insertNode(Node<V> item) {
        if (current == null) {
            current = item;
        } else {
            if (item != current) {
                Node<V> prev = current.prevNode;
                item.prevNode = prev;
                prev.nextNode = item;
                item.nextNode = current;
                current.prevNode = item;
                current = item;
            }
        }
    }
    
    public void appendNode(Node<V> item) {
        if (current == null) {
            current = item;
        } else {
            if (item != current) {
                Node<V> next = current.nextNode;
                item.nextNode = next;
                next.prevNode = item;
                current.nextNode = item;
                item.prevNode = current;
            }
        }
    }
    
    public boolean containsNode(Node<V> item) {
        if (item == null || current == null) {
            return false;
        }
        Node<V> node = item;
        do {
            if (node == null) {
                return false;
            }
            if (node == current) {
                return true;
            }
            node = node.nextNode;
        }
        while (node != item);
        return false;
    }
    
    public void removeNode(Node<V> item) {
        if (containsNode(item)) {
            if (item.prevNode != item.nextNode) {
                Node<V> prev = item.prevNode;
                Node<V> next = item.nextNode;
                prev.nextNode = next;
                next.prevNode = prev;
                item.nextNode = item;
                item.prevNode = item;
                if (current == item) {
                    current = next;
                }
            } else {
                if (current == item) {
                    current = null;
                }
            }
        }
    }

    public Node<V> next() {
        if (current == null) {
            return null;
        }
        Node<V> result = current;
        current = current.nextNode;
        return result;
    }
    
    public V nextRef() {
        Node<V> node = next();
        if (node == null) {
            return null;
        }
        return node.getRef();
    }
    
    public Node<V> prev() {
        if (current == null) {
            return null;
        }
        Node<V> result = current;
        current = current.prevNode;
        return result;
    }
    
    public V prevRef() {
        Node<V> node = prev();
        if (node == null) {
            return null;
        }
        return node.getRef();
    }
    
    public List<V> traverse() {
        List<V> list = new LinkedList<>();
        Node<V> pointer = current;
        if (pointer != null) {
            do {
                list.add(pointer.ref);
                pointer = pointer.nextNode;
            }
            while (pointer != current);
        }
        return list;
    }
    
    public static class Node<V> {
        private V ref;
        private Node<V> prevNode;
        private Node<V> nextNode;

        private Node(V ref) {
            this(null, ref, null);
        }
        
        private Node(Node<V> prevNode, V ref, Node<V> nextNode) {
            this.ref = ref;
            if (prevNode == null) {
                this.prevNode = this;
            } else {
                this.prevNode = prevNode;
                prevNode.nextNode = this;
            }
            if (nextNode == null) {
                this.nextNode = this;
            } else {
                this.nextNode = nextNode;
                nextNode.prevNode = this;
            }
        }

        public V getRef() {
            return ref;
        }

        public void setRef(V ref) {
            this.ref = ref;
        }
    }
}
