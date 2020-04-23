package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowCryptTool;
import com.devebot.opflow.supports.OpflowEnvTool;
import com.devebot.opflow.supports.OpflowStringUtil;
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

    private final Map<String, User> users;

    OpflowIdentityManager() {
        this.users = initUserMap((String[]) null);
    }

    OpflowIdentityManager(String credentials) {
        this.users = initUserMap(OpflowStringUtil.splitByComma(credentials));
    }

    OpflowIdentityManager(String[] userList) {
        this.users = initUserMap(userList);
    }

    OpflowIdentityManager(final Map<String, User> users) {
        this.users = applyRootCredentials(users);
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

    public boolean isActive() {
        return users.size() > 0;
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
            User user = users.get(account.getPrincipal().getName());
            if (user == null) return false;
            char[] expectedPassword = user.getPassword();
            if (expectedPassword == null) return false;
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
    
    private Map<String, User> initUserMap(String[] userList) {
        return applyRootCredentials(toUserMap(userList));
    }
    
    private Map<String, User> applyRootCredentials(Map<String, User> userMap) {
        return mergeUserMap(userMap, toUserMap(ENVTOOL.getEnvironVariable("OPFLOW_ROOT_CREDENTIALS", "")));
    }
    
    private Map<String, User> toUserMap(String credentials) {
        return toUserMap(OpflowStringUtil.splitByComma(credentials));
    }
    
    private Map<String, User> toUserMap(String[] userList) {
        Map<String, User> _users = new HashMap<>();
        if (userList != null) {
            for (String user : userList) {
                if (user != null) {
                    String[] pair = OpflowStringUtil.splitByDelimiter(user, String.class, ":");
                    if (pair.length == 2) {
                        _users.put(pair[0], new User(pair[1].toCharArray()));
                    }
                }
            }
        }
        return _users;
    }
    
    private Map<String, User> mergeUserMap(Map<String, User> target, Map<String, User>...sources) {
        if (target == null) {
            target = new HashMap<>();
        }
        if (sources != null) {
            for(Map<String, User> source : sources) {
                if (source == null) continue;
                for (Map.Entry<String, User> user : source.entrySet()) {
                    target.put(user.getKey(), user.getValue());
                }
            }
        }
        return target;
    }
    
    private class User {
        private final char[] password;

        public User(char[] password) {
            this.password = password;
        }

        public char[] getPassword() {
            return password;
        }
    }
}
