package com.devebot.opflow.supports;

/**
 *
 * @author drupalex
 */
public class OpflowMathUtil {
    private static final int[] RATE = new int[] { 0, 10, 100, 1000, 10000, 100000, 1000000 };
    
    public static double round(double d, int r) {
        if (r < 0 || r > 6) {
            return d;
        }
        if (r == 0) {
            return Math.round(d);
        }
        return Math.round(d * RATE[r]) / (double) RATE[r];
    }
}
