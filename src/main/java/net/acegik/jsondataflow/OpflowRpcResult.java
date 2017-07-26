package net.acegik.jsondataflow;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author drupalex
 */
public class OpflowRpcResult implements Iterator {

    public OpflowRpcResult() {
    }
    
    private final BlockingQueue<OpflowMessage> list = new LinkedBlockingQueue<OpflowMessage>();
    private OpflowMessage current = null;
    
    @Override
    public boolean hasNext() {
        try {
            this.current = list.take();
            if (isCompleted(this.current)) {
                return false;
            } else {
                return true;
            }
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
    }
    
    private final List<String> STATUS = Arrays.asList(new String[] { "failed", "completed" });
    
    private boolean isCompleted(OpflowMessage message) {
        Map<String, Object> info = message.getInfo();
        if (info == null) return false;
        return STATUS.indexOf(info.get("status")) >= 0;
    }
}
