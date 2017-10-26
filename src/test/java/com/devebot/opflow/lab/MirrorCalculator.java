package com.devebot.opflow.lab;

import com.devebot.opflow.annotation.OpflowRoutine;

/**
 *
 * @author drupalex
 */
public interface MirrorCalculator extends SimpleCalculator {
    @Override
    @OpflowRoutine(alias = {"increase"})
    Integer add(Integer a);
    @Override
    Integer add(Integer a, Integer b);
}
