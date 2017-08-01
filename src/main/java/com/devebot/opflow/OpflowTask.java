package com.devebot.opflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowTask {
    
    public static class Timeout extends Thread {

        final Logger logger = LoggerFactory.getLogger(Timeout.class);
        
        public Timeout(int max, Listener listener) {
            this.max = max;
            this.listener = listener;
        }

        public Timeout(int interval, int max, Listener listener) {
            this(max, listener);
            this.interval = interval;
        }

        private Listener listener;
        private int interval = 1000;
        private int max = 0;
        private int count = 0;
        private boolean done = false;

        @Override
        public void run() {
            while(count < max && !done) {
                try {
                    Thread.sleep(interval);
                    count += 1;
                    if (logger.isTraceEnabled()) logger.trace("Check " + count + "/" + max);
                    if (count >= max) {
                        if (this.listener != null) {
                            listener.handleEvent();
                        }
                        if (logger.isTraceEnabled()) logger.trace("Thread interrupted");
                        this.interrupt();
                    }
                } catch(InterruptedException ie) {}
            }
        }

        public void check() {
            this.count = 0;
        }

        public void close() {
            this.done = true;
            if (logger.isTraceEnabled()) logger.trace("Thread is closed gracefully");
        }
    }
    
    public interface Listener {
         public void handleEvent();
    }
}
