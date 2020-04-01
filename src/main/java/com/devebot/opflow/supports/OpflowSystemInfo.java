package com.devebot.opflow.supports;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author pnhung177
 */
public class OpflowSystemInfo {
    
    public static final String GIT_INFO_FILE = "META-INF/scm/com.devebot.opflow/opflow-core/git-info.json";
    
    public static int getNumberOfProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }
    
    public static Long getPid() {
        try {
            String processName = ManagementFactory.getRuntimeMXBean().getName();
            return Long.parseLong(processName.split("@")[0]);
        }
        catch (Exception e) {
            return null;
        }
    }

    public static Map<String, Object> getGitInfo() {
        return getGitInfo(null);
    }
    
    public static Map<String, Object> getGitInfo(String gitInfoFile) {
        if (gitInfoFile == null) {
            gitInfoFile = GIT_INFO_FILE;
        }
        try {
            return OpflowJsonTool.toObject(OpflowSystemInfo.class.getClassLoader().getResourceAsStream(gitInfoFile), Map.class);
        } catch (Exception ioe) {
            return new HashMap<>();
        }
    }
    
    public static OsInfo getOsInfo() {
        if (osInfo == null) {
            osInfo = new OsInfo();
        }
        return osInfo;
    }
    
    public static OsInfo osInfo = null;
    
    public static class OsInfo {
        private final int availableProcessors;
        private final String arch;
        private final String name;
        private final String version;

        public OsInfo() {
            OperatingSystemMXBean operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            availableProcessors = operatingSystemMXBean.getAvailableProcessors();
            arch = operatingSystemMXBean.getArch();
            name = operatingSystemMXBean.getName();
            version = operatingSystemMXBean.getVersion();
        }

        public int getAvailableProcessors() {
            return availableProcessors;
        }

        public String getArch() {
            return arch;
        }

        public String getName() {
            return name;
        }

        public String getVersion() {
            return version;
        }
    }
    
    public static CpuUsage getCpuUsage() {
        return new CpuUsage();
    }
    
    public static class CpuUsage {
        private final double processCpuLoad;
        private final double systemCpuLoad;
        private final double systemLoadAverage;
        
        public CpuUsage() {
            OperatingSystemMXBean operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            processCpuLoad = operatingSystemMXBean.getProcessCpuLoad();
            systemCpuLoad = operatingSystemMXBean.getSystemCpuLoad();
            systemLoadAverage = operatingSystemMXBean.getSystemLoadAverage();
        }

        public double getProcessCpuLoad() {
            return processCpuLoad;
        }

        public double getSystemCpuLoad() {
            return systemCpuLoad;
        }

        public double getSystemLoadAverage() {
            return systemLoadAverage;
        }
    }
    
    public static MemUsage getMemUsage() {
        return new MemUsage();
    }
    
    public static class MemUsage {
        public final static long SIZE_RATE = 1024L;
        
        public final static String FREE_HEAP_SIZE = "freeHeapSize";
        public final static String USED_HEAP_SIZE = "usedHeapSize";
        public final static String CURRENT_HEAP_SIZE = "currentHeapSize";
        public final static String MAXIMUM_HEAP_SIZE = "maximumHeapSize";
        public final static String SIZE_INFO_SUFFIX = "Formatted";
        
        private final long freeMemory;
        private final long usedMemory;
        private final long currentMemory;
        private final long maximumMemory;

        public MemUsage() {
            Runtime runtime = Runtime.getRuntime();
            maximumMemory = runtime.maxMemory();
            currentMemory = runtime.totalMemory();
            freeMemory = runtime.freeMemory();
            usedMemory = currentMemory - freeMemory;
        }
        
        public Map<String, Object> toMap() {
            return OpflowObjectTree.buildMap()
                .put(USED_HEAP_SIZE, usedMemory)
                .put(USED_HEAP_SIZE + SIZE_INFO_SUFFIX, formatSize(usedMemory))
                .put(FREE_HEAP_SIZE, freeMemory)
                .put(FREE_HEAP_SIZE + SIZE_INFO_SUFFIX, formatSize(freeMemory))
                .put(CURRENT_HEAP_SIZE, currentMemory)
                .put(CURRENT_HEAP_SIZE + SIZE_INFO_SUFFIX, formatSize(currentMemory))
                .put(MAXIMUM_HEAP_SIZE, maximumMemory)
                .put(MAXIMUM_HEAP_SIZE + SIZE_INFO_SUFFIX, formatSize(maximumMemory))
                .toMap();
        }
        
        public Map<String, Object> toMap(boolean humanReadable) {
            if (humanReadable) {
                return OpflowObjectTree.buildMap()
                    .put(USED_HEAP_SIZE, formatSize(usedMemory))
                    .put(FREE_HEAP_SIZE, formatSize(freeMemory))
                    .put(CURRENT_HEAP_SIZE, formatSize(currentMemory))
                    .put(MAXIMUM_HEAP_SIZE, formatSize(maximumMemory))
                    .toMap();
            }
            return OpflowObjectTree.buildMap()
                .put(USED_HEAP_SIZE, usedMemory)
                .put(FREE_HEAP_SIZE, freeMemory)
                .put(CURRENT_HEAP_SIZE, currentMemory)
                .put(MAXIMUM_HEAP_SIZE, maximumMemory)
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
