package net.acegik.jsondataflow;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author drupalex
 */
public class OpflowRpcResult implements Iterator {

    private final String id;
    private final String requestId;
    private final String routineId;
    
    public OpflowRpcResult() {
        this(null, null);
    }
    
    public OpflowRpcResult(String routineId) {
        this(routineId, null);
    }
    
    public OpflowRpcResult(String routineId, String requestId) {
        this.id = UUID.randomUUID().toString();
        this.requestId = requestId != null ? requestId : UUID.randomUUID().toString();
        this.routineId = routineId != null ? routineId : UUID.randomUUID().toString();
    }
    
    public String getId() {
        return id;
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
            return !isCompleted(this.current);
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
    
    private static final List<String> STATUS = Arrays.asList(new String[] { "failed", "completed" });
    
    public static boolean isCompleted(OpflowMessage message) {
        Map<String, Object> info = message.getInfo();
        if (info == null) return false;
        String status = info.get("status").toString();
        return STATUS.indexOf(status) >= 0;
    }
}
