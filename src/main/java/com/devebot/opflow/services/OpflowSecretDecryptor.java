package com.devebot.opflow.services;

import com.devebot.jigsaw.vault.core.VaultCryptor;
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
    
    private final VaultCryptor cryptor = new VaultCryptor();
    
    @Override
    public Map<String, Object> transform(Map<String, Object> config) throws OpflowBootstrapException {
        return OpflowObjectTree.traverseTree(config, new OpflowObjectTree.LeafUpdater() {
            @Override
            public Object transform(String[] path, Object value) {
                if (value instanceof String) {
                    String valueStr = (String) value;
                    if (cryptor.isVaultBlock(valueStr)) {
                        return cryptor.decryptVault(valueStr);
                    }
                    return valueStr;
                }
                return value;
            }
        });
    }
}
