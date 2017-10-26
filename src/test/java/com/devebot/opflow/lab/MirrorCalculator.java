package com.devebot.opflow.lab;

import com.devebot.opflow.annotation.OpflowTargetRoutine;
import com.devebot.opflow.annotation.OpflowSourceRoutine;

/**
 *
 * @author drupalex
 */
public interface MirrorCalculator extends SimpleCalculator {
    @Override
    @OpflowSourceRoutine(alias = "add_by_1")
    @OpflowTargetRoutine(alias = {"increase"})
    Integer add(Integer a);
    @Override
    Integer add(Integer a, Integer b);
}
