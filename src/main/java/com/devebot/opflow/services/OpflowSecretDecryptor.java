package com.devebot.opflow.services;

import com.devebot.opflow.OpflowConfig;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowObjectTree;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowSecretDecryptor implements OpflowConfig.Transformer {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowSecretDecryptor.class);
    
    @Override
    public Map<String, Object> transform(Map<String, Object> config) throws OpflowBootstrapException {
        return OpflowObjectTree.traverseTree(config, new OpflowObjectTree.LeafUpdater() {
            @Override
            public Object transform(String[] path, Object value) {
                return value;
            }
        });
    }
}
