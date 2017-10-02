package com.devebot.opflow.lab;

/**
 *
 * @author drupalex
 */
public interface SimpleCalculator {

    Integer tick() throws SimpleCalculatorException;

    Integer add(Integer a);

    Integer add(Integer a, Integer b);

    void printInfo();
    
}
