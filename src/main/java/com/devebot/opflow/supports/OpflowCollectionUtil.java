package com.devebot.opflow.supports;

import java.util.Arrays;
import java.util.HashSet;

/**
 *
 * @author acegik
 */
public class OpflowCollectionUtil {
    public static String[] distinct(String[] source) {
        return (new HashSet<String>(Arrays.asList(source))).toArray(new String[0]);
    }
}
