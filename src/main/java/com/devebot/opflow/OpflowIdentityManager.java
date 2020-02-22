package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowCryptTool;
import com.devebot.opflow.supports.OpflowEnvTool;
import io.undertow.security.api.SecurityContext;
import io.undertow.security.idm.Account;
import io.undertow.security.idm.Credential;
import io.undertow.security.idm.IdentityManager;
import io.undertow.security.idm.PasswordCredential;
import io.undertow.server.HttpServerExchange;

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author acegik
 */
public class OpflowIdentityManager implements IdentityManager {

    private final static OpflowEnvTool ENVTOOL = OpflowEnvTool.instance;

    private final Map<String, char[]> users;

    OpflowIdentityManager() {
        this.users = initUserMap();
    }

    OpflowIdentityManager(final Map<String, char[]> users) {
        this.users = users;
    }

    @Override
    public Account verify(Account account) {
        // An existing account so for testing assume still valid.
        return account;
    }

    @Override
    public Account verify(String id, Credential credential) {
        Account account = getAccount(id);
        if (account != null && verifyCredential(account, credential)) {
            return account;
        }

        return null;
    }

    @Override
    public Account verify(Credential credential) {
        // TODO Auto-generated method stub
        return null;
    }
    
    public static String getUsername(HttpServerExchange exchange) {
        final SecurityContext context = exchange.getSecurityContext();
        if (context == null) return null;
        final Account account = context.getAuthenticatedAccount();
        if (account == null) return null;
        final Principal principal = account.getPrincipal();
        if (principal == null) return null;
        return principal.getName();
    }

    private boolean verifyCredential(Account account, Credential credential) {
        if (credential instanceof PasswordCredential) {
            char[] password = ((PasswordCredential) credential).getPassword();
            char[] expectedPassword = users.get(account.getPrincipal().getName());
            return OpflowCryptTool.checkPasswd(password, expectedPassword);
        }
        return false;
    }

    private Account getAccount(final String id) {
        if (users.containsKey(id)) {
            return new Account() {

                private final Principal principal = new Principal() {
                    @Override
                    public String getName() {
                        return id;
                    }
                };

                @Override
                public Principal getPrincipal() {
                    return principal;
                }

                @Override
                public Set<String> getRoles() {
                    return Collections.emptySet();
                }
            };
        }
        return null;
    }
    
    private Map<String, char[]> initUserMap() {
        String[] propUsers = OpflowUtil.splitByComma(ENVTOOL.getSystemProperty("OPFLOW_SUPERADMIN", ""));
        if (propUsers == null || propUsers.length == 0) {
            propUsers = new String[] { "master:$2y$05$nntUsCdr33UxrV3UfWcVSOWHKIoEO.V/guH3aymzvC5IWfxjwffom" };
        }
        return toUserMap(propUsers);
    }
    
    private Map<String, char[]> toUserMap(String[] userList) {
        Map<String, char[]> users = new HashMap<>();
        if (userList != null) {
            for (String user : userList) {
                if (user != null) {
                    String[] pair = OpflowUtil.splitByComma(user, String.class, ":");
                    if (pair.length == 2) {
                        users.put(pair[0], pair[1].toCharArray());
                    }
                }
            }
        }
        return users;
    }
}
