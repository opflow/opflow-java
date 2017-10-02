package com.devebot.opflow.lab;

import com.devebot.opflow.annotation.OpflowRoutine;

/**
 *
 * @author drupalex
 */
public class FibonacciCalculatorImpl implements FibonacciCalculator {

    @Override
    @OpflowRoutine(alias={"fibonacci1"})
    public FibonacciResult calc(int number) {
        return new FibonacciGenerator(number).finish();
    }

    @Override
    @OpflowRoutine(alias={"fibonacci2"}, enabled=true)
    public FibonacciResult calc(FibonacciPacket data) {
        return this.calc(data.getNumber());
    }
}
