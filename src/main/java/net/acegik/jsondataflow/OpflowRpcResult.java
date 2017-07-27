package net.acegik.jsondataflow;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowRpcResult implements Iterator {

    final Logger logger = LoggerFactory.getLogger(OpflowRpcResult.class);
    private final String requestId;
    private final String routineId;
    private final Integer timeout;
    private OpflowTimeout timeoutWatcher;
    private OpflowTimeout.Listener completeListener;
    
    public OpflowRpcResult(Map<String, Object> opts, final OpflowTimeout.Listener completeListener) {
        this.requestId = (opts != null && opts.get("requestId") != null) ? 
                (String)opts.get("requestId") : UUID.randomUUID().toString();
        this.routineId = (opts != null && opts.get("routineId") != null) ? 
                (String)opts.get("routineId") : UUID.randomUUID().toString();
        this.timeout = (opts != null) ? (Integer)opts.get("timeout") : null;
        this.completeListener = completeListener;
        if (this.timeout > 0 && completeListener != null) {
            timeoutWatcher = new OpflowTimeout(this.timeout, new OpflowTimeout.Listener() {
                @Override
                public void handleEvent() {
                    if (logger.isDebugEnabled()) logger.debug("timeout event has been raised");
                    list.add(OpflowMessage.ERROR);
                    if (logger.isDebugEnabled()) logger.debug("raise completeListener (timeout)");
                    completeListener.handleEvent();
                }
            });
            timeoutWatcher.start();
        }
    }

    public String getRequestId() {
        return requestId;
    }
    
    public String getRoutineId() {
        return routineId;
    }
    
    private final BlockingQueue<OpflowMessage> list = new LinkedBlockingQueue<OpflowMessage>();
    private OpflowMessage current = null;
    
    @Override
    public boolean hasNext() {
        try {
            this.current = list.take();
            if (this.current == OpflowMessage.EMPTY) return false;
            if (this.current == OpflowMessage.ERROR) return false;
            return true;
        } catch (InterruptedException ie) {
            return false;
        }
    }

    @Override
    public OpflowMessage next() {
        OpflowMessage result = this.current;
        this.current = null;
        return result;
    }
    
    public void push(OpflowMessage message) {
        list.add(message);
        if (timeoutWatcher != null) {
            timeoutWatcher.check();
        }
        if(isCompleted(message)) {
            if (logger.isDebugEnabled()) logger.debug("completed/failed message");
            list.add(OpflowMessage.EMPTY);
            if (completeListener != null) {
                if (logger.isDebugEnabled()) logger.debug("raise completeListener (completed)");
                completeListener.handleEvent();
            }
            if (timeoutWatcher != null) {
                timeoutWatcher.close();
            }
        }
    }
    
    private static final List<String> STATUS = Arrays.asList(new String[] { "failed", "completed" });
    
    public boolean isCompleted(OpflowMessage message) {
        Map<String, Object> info = message.getInfo();
        if (info == null) return false;
        String status = info.get("status").toString();
        return STATUS.indexOf(status) >= 0;
    }
}
