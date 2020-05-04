package com.devebot.opflow.supports;

import at.favre.lib.nostro.crypto.bcrypt.BCrypt;
import com.devebot.jigsaw.vault.core.VaultCryptor;
import java.util.Properties;

/**
 *
 * @author acegik
 */
public class OpflowCryptTool {
    public static boolean checkPasswd(String password, String hash) {
        if (password == null) return false;
        if (hash == null) return false;
        return checkPasswd(password.toCharArray(), hash.toCharArray());
    }
    
    public static boolean checkPasswd(char[] password, char[] hash) {
        BCrypt.Result result = BCrypt.verifyer().verify(password, hash);
        return result.verified;
    }
    
    public static Properties decryptVault(Properties source) {
        if (source == null) {
            return null;
        }
        for(String key: source.stringPropertyNames()) {
            String valueStr = source.getProperty(key);
            if (getVaultCryptor().isVaultBlock(valueStr)) {
                source.setProperty(key, getVaultCryptor().decryptVault(valueStr));
            }
        }
        return source;
    }
    
    private static VaultCryptor vaultCryptor = null;
    
    public static VaultCryptor getVaultCryptor() {
        if (vaultCryptor == null) {
            synchronized (VaultCryptor.class) {
                if (vaultCryptor == null) {
                    vaultCryptor = new VaultCryptor();
                }
            }
        }
        return vaultCryptor;
    }
}
