package com.devebot.opflow.supports;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author pnhung177
 */
public class OpflowSysInfo {
    
    public static final String GIT_INFO_FILE = "META-INF/scm/com.devebot.opflow/opflow-core/git-info.json";
    
    public static int getNumberOfProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }
    
    public static Map<String, Object> getGitInfo() {
        return getGitInfo(null);
    }
    
    public static Map<String, Object> getGitInfo(String gitInfoFile) {
        if (gitInfoFile == null) {
            gitInfoFile = GIT_INFO_FILE;
        }
        try {
            return OpflowJsonTool.toObject(OpflowSysInfo.class.getClassLoader().getResourceAsStream(gitInfoFile), Map.class);
        } catch (Exception ioe) {
            return new HashMap<>();
        }
    }
}
