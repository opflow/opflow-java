package com.devebot.opflow.lab;

import com.devebot.opflow.annotation.OpflowRoutine;

/**
 *
 * @author drupalex
 */
public class SimpleCalculatorImpl implements SimpleCalculator {
    
    int count = 0;

    @Override
    public Integer tick() throws SimpleCalculatorException {
        ++count;
        if (count > 1) {
            throw new SimpleCalculatorException("this is a demo");
        }
        return count;
    }

    @Override
    @OpflowRoutine(alias = {"increase"})
    public Integer add(Integer a) {
        return a + 1;
    }

    @Override
    public Integer add(Integer a, Integer b) {
        return a + b;
    }

    @Override
    public void printInfo() {
        System.out.println("Hello world");
    }
    
}
