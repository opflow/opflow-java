package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.lab.GenericCalculator;
import com.devebot.opflow.lab.SimpleCalculator;
import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

/**
 *
 * @author drupalex
 */
public class OpflowUtilTest {
    
    @Test
    public void test_getLogID() {
        String uuid1 = "f487fbcf-3606-4f35-a23d-5ac3af6de754";
        String logId1 = OpflowUtil.getLogID(uuid1);
        System.out.println("logId1: " + uuid1 + " / " + logId1);
        MatcherAssert.assertThat(logId1, Matchers.equalTo("9If7zzYGTzWiPVrDr23nVA"));
    }
    
    @Test
    public void test_isGenericDeclaration() {
        Assert.assertTrue(OpflowUtil.isGenericDeclaration(GenericCalculator.class.toGenericString()));
        Assert.assertFalse(OpflowUtil.isGenericDeclaration(SimpleCalculator.class.toGenericString()));
    }
    
    @Test
    public void test_getAllAncestorTypes() {
        List<Class<?>> ancestorTypes = OpflowUtil.getAllAncestorTypes(HashMap.class);
        for(Class clazz: ancestorTypes) {
            System.out.println("Type: " + clazz.getName());
        }
        MatcherAssert.assertThat(ancestorTypes, Matchers.containsInAnyOrder(new Class[] {
            java.util.Map.class,
            java.lang.Cloneable.class,
            java.io.Serializable.class,
            java.util.AbstractMap.class,
            java.util.HashMap.class
        }));
    }
}
