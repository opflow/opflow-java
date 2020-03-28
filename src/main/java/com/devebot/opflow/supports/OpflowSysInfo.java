package com.devebot.opflow.supports;

import com.devebot.opflow.OpflowConstant;
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
    
    public static HeapInfo getHeapInfo() {
        return new HeapInfo();
    }
    
    public static class HeapInfo {
        public final static long SIZE_RATE = 1024L;
        public final static String SIZE_INFO_SUFFIX = "Formatted";
        
        private final long freeMemory;
        private final long usedMemory;
        private final long currentMemory;
        private final long maximumMemory;

        public HeapInfo() {
            Runtime runtime = Runtime.getRuntime();
            maximumMemory = runtime.maxMemory();
            currentMemory = runtime.totalMemory();
            freeMemory = runtime.freeMemory();
            usedMemory = currentMemory - freeMemory;
        }
        
        public Map<String, Object> toMap() {
            return OpflowObjectTree.buildMap()
                .put(OpflowConstant.OPFLOW_COMMON_USED_HEAP_SIZE, usedMemory)
                .put(OpflowConstant.OPFLOW_COMMON_USED_HEAP_SIZE + SIZE_INFO_SUFFIX, formatSize(usedMemory))
                .put(OpflowConstant.OPFLOW_COMMON_FREE_HEAP_SIZE, freeMemory)
                .put(OpflowConstant.OPFLOW_COMMON_FREE_HEAP_SIZE + SIZE_INFO_SUFFIX, formatSize(freeMemory))
                .put(OpflowConstant.OPFLOW_COMMON_CURRENT_HEAP_SIZE, currentMemory)
                .put(OpflowConstant.OPFLOW_COMMON_CURRENT_HEAP_SIZE + SIZE_INFO_SUFFIX, formatSize(currentMemory))
                .put(OpflowConstant.OPFLOW_COMMON_MAXIMUM_HEAP_SIZE, maximumMemory)
                .put(OpflowConstant.OPFLOW_COMMON_MAXIMUM_HEAP_SIZE + SIZE_INFO_SUFFIX, formatSize(maximumMemory))
                .toMap();
        }
        
        private String formatSize(long sizeInBytes) {
            long sizeMB = 0;
            long sizeKB = sizeInBytes / SIZE_RATE;
            if (sizeKB > SIZE_RATE) {
                sizeMB = sizeKB / SIZE_RATE;
            }
            if (sizeMB > 0) {
                return "" + sizeMB + " MB";
            }
            return "" + sizeKB + " KB";
        }
    }
}
