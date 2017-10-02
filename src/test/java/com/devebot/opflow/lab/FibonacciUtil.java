package com.devebot.opflow.lab;

import java.util.Random;

/**
 *
 * @author drupalex
 */
public class FibonacciUtil {
    private static final Random RANDOM = new Random();
    
    public static int random(int min, int max) {
        return RANDOM.nextInt(max + 1 - min) + min;
    }
}
