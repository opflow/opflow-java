package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowTask;
import com.devebot.opflow.OpflowUtil;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 *
 * @author drupalex
 */
public class OpflowTaskTest {
    private Map<String, TimeoutExample> timeoutables;
    private OpflowTask.TimeoutMonitor timeoutHandler;

    @Rule public TestName name = new TestName();
    
    private List<String> group1Names = Arrays.asList(new String[] {"testTimeout"});
    
    @Before
    public void group1Setup() {
        Assume.assumeTrue(group1Names.indexOf(name.getMethodName()) >= 0);
        timeoutables = new ConcurrentHashMap<String, TimeoutExample>();
        timeoutHandler = new OpflowTask.TimeoutMonitor(timeoutables, 1000, 3000);
    }
    
    @Test
    public void testTimeout() {
        List<Integer> timeoutTasks = Arrays.asList(new Integer[] {3, 5, 7, 9, 91, 95});
        List<Integer> updatedTasks = Arrays.asList(new Integer[] {5, 7});
        timeoutHandler.start();
        for(int i=0; i<100; i++) {
            if (timeoutTasks.indexOf(i) >= 0) {
                timeoutables.put("task#" + i, new TimeoutExample(2000));
            } else {
                timeoutables.put("task#" + i, new TimeoutExample(30000));
            }
        }
        for(int j=0; j<3; j++) {
            sleep(1500);
            for(Integer idx:updatedTasks) {
                TimeoutExample item = timeoutables.get("task#" + idx);
                item.updateData("something");
            }
        }
        Assert.assertEquals(100 - timeoutTasks.size() + updatedTasks.size(), timeoutables.size());
        Assert.assertEquals(timeoutTasks.size() - updatedTasks.size(), thrownItems);
        timeoutHandler.stop();
    }
    
    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch(InterruptedException ie) {}
    }
    
    public static int thrownItems = 0;
    
    public class TimeoutExample implements OpflowTask.Timeoutable {

        private final long timeout;
        private long timestamp;
        
        public TimeoutExample() {
            this(0);
        }
        
        public TimeoutExample(long timeout) {
            this.timeout = timeout;
            this.timestamp = OpflowUtil.getCurrentTime();
        }
        
        @Override
        public long getTimeout() {
            return timeout;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public void raiseTimeout() {
            thrownItems++;
        }
        
        public void updateData(String data) {
            // reset timestamp
            this.timestamp = OpflowUtil.getCurrentTime();
            // do something to update data ...
        }
    }
}

