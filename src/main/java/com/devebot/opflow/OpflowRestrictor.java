package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowRequestSuspendException;
import com.devebot.opflow.exception.OpflowRequestWaitingException;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 *
 * @author pnhung177
 */
public class OpflowRestrictor {
    public interface Action<T> {
        public T process();
    }
    
    private final ReadWriteLock pauseLock;
    private long pauseTimeout;
    
    private final int semaphoreLimit;
    private boolean semaphoreEnabled;
    private long semaphoreTimeout;
    
    private final Semaphore semaphore;
    
    public OpflowRestrictor(ReadWriteLock pushLock, int semaphoreLimit) {
        this(pushLock, semaphoreLimit, null);
    }
    
    public OpflowRestrictor(ReadWriteLock pushLock, int semaphoreLimit, Map<String, Object> options) {
        options = OpflowUtil.ensureNotNull(options);
        
        this.pauseLock = pushLock;
        
        if (options.get("pauseTimeout") instanceof Long) {
            pauseTimeout = (Long) options.get("pauseTimeout");
        } else {
            pauseTimeout = 30000;
        }
        
        this.semaphoreLimit = semaphoreLimit;
        this.semaphore = new Semaphore(this.semaphoreLimit);
        
        if (options.get("semaphoreEnabled") instanceof Boolean) {
            semaphoreEnabled = (Boolean) options.get("semaphoreEnabled");
        } else {
            semaphoreEnabled = true;
        }
        
        if (options.get("semaphoreTimeout") instanceof Long) {
            semaphoreTimeout = (Long) options.get("semaphoreTimeout");
        } else {
            semaphoreTimeout = 30000;
        }
    }
    
    public <T> T filter(Action<T> action) {
        Lock rl = pauseLock.readLock();
        if (pauseTimeout > 0) {
            try {
                if (rl.tryLock(pauseTimeout, TimeUnit.MILLISECONDS)) {
                    try {
                        return _filter(action);
                    }
                    finally {
                        rl.unlock();
                    }
                } else {
                    throw new OpflowRequestSuspendException("tryLock() return false - the lock is not available");
                }
            }
            catch (InterruptedException exception) {
                throw new OpflowRequestSuspendException("tryLock() is interrupted", exception);
            }
        } else {
            rl.lock();
            try {
                return _filter(action);
            }
            finally {
                rl.unlock();
            }
        }
    }
    
    private <T> T _filter(Action<T> action) {
        if (semaphoreEnabled) {
            try {
                if (semaphoreTimeout > 0) {
                    if (semaphore.tryAcquire(semaphoreTimeout, TimeUnit.MILLISECONDS)) {
                        try {
                            return action.process();
                        }
                        finally {
                            semaphore.release();
                        }
                    } else {
                        throw new OpflowRequestWaitingException("There are no permits available");
                    }
                } else {
                    semaphore.acquire();
                    try {
                        return action.process();
                    }
                    finally {
                        semaphore.release();
                    }
                }
            }
            catch (InterruptedException exception) {
                throw new OpflowRequestWaitingException("semaphore.acquire() is interrupted", exception);
            }
        } else {
            return action.process();
        }
    }
}
