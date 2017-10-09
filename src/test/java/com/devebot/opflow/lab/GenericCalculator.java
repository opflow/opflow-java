package com.devebot.opflow.lab;

/**
 *
 * @author drupalex
 */
public interface GenericCalculator<T> {
    Integer tick() throws SimpleCalculatorException;
    T add(T a);
    T add(T a, T b);
    void printInfo();
}
