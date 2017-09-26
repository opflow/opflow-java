package com.devebot.opflow.lab;

/**
 *
 * @author drupalex
 */
public class FibonacciCalculatorImpl implements FibonacciCalculator {

    @Override
    public FibonacciResult calc(int number) {
        return new FibonacciGenerator(number).finish();
    }

    @Override
    public FibonacciResult calc(FibonacciPacket data) {
        return this.calc(data.getNumber());
    }
}
