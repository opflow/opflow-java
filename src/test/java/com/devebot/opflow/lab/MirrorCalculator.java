package com.devebot.opflow.lab;

import com.devebot.opflow.annotation.OpflowRoutineSource;
import com.devebot.opflow.annotation.OpflowRoutineTarget;

/**
 *
 * @author drupalex
 */
public interface MirrorCalculator extends SimpleCalculator {
    @Override
    @OpflowRoutineSource(alias = "add_by_1")
    @OpflowRoutineTarget(alias = {"increase"})
    Integer add(Integer a);
    @Override
    Integer add(Integer a, Integer b);
}
