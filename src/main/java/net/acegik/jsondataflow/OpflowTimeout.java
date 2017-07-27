package net.acegik.jsondataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowTimeout extends Thread {
    
    final Logger logger = LoggerFactory.getLogger(OpflowTimeout.class);
    
    public OpflowTimeout(int max, Listener listener) {
        this.max = max;
        this.listener = listener;
    }

    public OpflowTimeout(int interval, int max, Listener listener) {
        this(max, listener);
        this.interval = interval;
    }

    private Listener listener;
    private int interval = 1000;
    private int max = 0;
    private int count = -1;
    private boolean done = false;

    @Override
    public void run() {
        while(count < max && !done) {
            try {
                if (logger.isTraceEnabled()) logger.trace("Check " + count + "/" + max);
                Thread.sleep(interval);
                count += 1;
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
    
    public interface Listener {
         public void handleEvent();
    }
}
