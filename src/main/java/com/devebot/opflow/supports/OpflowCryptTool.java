package com.devebot.opflow.supports;

import at.favre.lib.crypto.bcrypt.BCrypt;


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
}
