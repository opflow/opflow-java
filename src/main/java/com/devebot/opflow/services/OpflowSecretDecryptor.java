package com.devebot.opflow.services;

import com.devebot.jigsaw.vault.core.VaultHandler;
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
    
    private final VaultHandler handler = new VaultHandler();
    
    @Override
    public Map<String, Object> transform(Map<String, Object> config) throws OpflowBootstrapException {
        return OpflowObjectTree.traverseTree(config, new OpflowObjectTree.LeafUpdater() {
            @Override
            public Object transform(String[] path, Object value) {
                if (value instanceof String) {
                    String valueStr = (String) value;
                    if (handler.isVaultBlock(valueStr)) {
                        return handler.decryptVault(valueStr);
                    }
                    return valueStr;
                }
                return value;
            }
        });
    }
}
