package net.acegik.examples;

/**
 *
 * @author drupalex
 */
public class FibonacciGenerator {
    private int n;
    private int c = 0;
    private long f = 0, f_1 = 0, f_2 = 0;
    
    public FibonacciGenerator(int number) {
        this.n = number;
    }
    
    public boolean next() {
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
}
