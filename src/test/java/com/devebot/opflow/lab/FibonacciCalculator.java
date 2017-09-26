package com.devebot.opflow.lab;

/**
 *
 * @author drupalex
 */
public interface FibonacciCalculator {
    FibonacciResult calc(int number);
    FibonacciResult calc(FibonacciPacket data);
}
