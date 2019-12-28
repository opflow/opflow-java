package com.devebot.opflow;

/**
 *
 * @author drupalex
 */
public interface OpflowTaskSubmitter {
    void pause(long duration);
    void reset();
}
