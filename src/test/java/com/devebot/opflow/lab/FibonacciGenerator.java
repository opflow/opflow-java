package com.devebot.opflow.lab;

import java.util.Random;

/**
 *
 * @author drupalex
 */
public class FibonacciGenerator {
    private int n;
    private int c = 0;
    private long f = 0, f_1 = 0, f_2 = 0;
    private int m;
    private int M;
    
    public FibonacciGenerator(int number) {
        this(number, 0);
    }
    
    public FibonacciGenerator(int number, int max) {
        this(number, 0, max);
    }
    
    public FibonacciGenerator(int number, int min, int max) {
        this.n = number;
        this.m = min;
        this.M = max;
    }
    
    public boolean next() {
        if (0 <= this.m && this.m < this.M) {
            int d = random(this.m, this.M);
            if (d > 0) {
                try {
                    Thread.sleep(d);
                } catch(InterruptedException ie) {}
            }
        }
        if (c >= n) return false;
        if (++c < 2) {
            f = c;
        } else {
            f_2 = f_1; f_1 = f; f = f_1 + f_2;
        }
        return true;
    }
    
    public Result result() {
        return new Result(f, c, n);
    }
    
    public Result finish() {
        while(next()) {}
        return result();
    }
    
    public class Result {
        private final long value;
        private final int step;
        private final int number;

        public Result(long value, int step, int number) {
            this.value = value;
            this.step = step;
            this.number = number;
        }

        public long getValue() {
            return value;
        }

        public int getStep() {
            return step;
        }

        public int getNumber() {
            return number;
        }
    }
    
    private static final Random RANDOM = new Random();
    
    private static int random(int min, int max) {
        return RANDOM.nextInt(max + 1 - min) + min;
    }
}
