package com.devebot.opflow.supports;

import com.devebot.opflow.OpflowUtil;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author drupalex
 */
public class OpflowTextFormat {
    private static final String PLACEHOLDER_START = "\\$\\{";
    private static final String PLACEHOLDER_END = "\\}";

    private static final String PH_STR = PLACEHOLDER_START + "([^}]+)" + PLACEHOLDER_END;
    private static final Pattern PH_PATTERN = Pattern.compile(PH_STR);

    public static String format(String format, Map<String, Object> params) {
        Matcher m = PH_PATTERN.matcher(format);
        String result = format;
        while (m.find()) {
            String[] fieldPath = m.group(1).split("\\.");
            Object newObj = OpflowUtil.getOptionField(params, fieldPath);
            String newVal = (newObj == null) ? "<null>" : newObj.toString();
            result = result.replaceFirst(PH_STR, newVal);
        }
        return result;
    }
}
