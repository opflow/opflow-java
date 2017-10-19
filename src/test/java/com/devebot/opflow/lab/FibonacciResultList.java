package com.devebot.opflow.lab;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author drupalex
 */
public class FibonacciResultList {
    private final List<FibonacciResult> list = new ArrayList<FibonacciResult>();

    public FibonacciResultList() {
    }

    public FibonacciResultList(List<FibonacciResult> init) {
        list.addAll(init);
    }

    public List<FibonacciResult> getList() {
        List<FibonacciResult> copied = new ArrayList<FibonacciResult>();
        copied.addAll(list);
        return copied;
    }
    
    public void add(FibonacciResult result) {
        list.add(result);
    }
}
