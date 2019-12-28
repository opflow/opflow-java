package com.devebot.opflow;

import java.util.Map;

/**
 *
 * @author drupalex
 */
public interface OpflowTaskSubmitter {
    Map<String, Object> pause(long duration);
    Map<String, Object> unpause();
    Map<String, Object> reset();
}
