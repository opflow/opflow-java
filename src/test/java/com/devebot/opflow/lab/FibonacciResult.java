package com.devebot.opflow.lab;

/**
 *
 * @author drupalex
 */
public class FibonacciResult {
    
    private final long value;
    private final int step;
    private final int number;

    public FibonacciResult(long value, int step, int number) {
        this.value = value;
        this.step = step;
        this.number = number;
    }

    public long getValue() {
        return value;
    }

    public int getStep() {
        return step;
    }

    public int getNumber() {
        return number;
    }
}
