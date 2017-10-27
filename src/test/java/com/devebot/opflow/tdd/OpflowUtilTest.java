package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.lab.GenericCalculator;
import com.devebot.opflow.lab.SimpleCalculator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

/**
 *
 * @author drupalex
 */
public class OpflowUtilTest {
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
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
    
    @Test
    public void test_splitByComma() {
        Integer[] numbers = OpflowUtil.splitByComma("0,1,2,3,4,5,6,7,8,9", Integer.class);
        MatcherAssert.assertThat(numbers, Matchers.equalTo(new Integer[] {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9
        }));
    }
    
    @Test
    public void test_getOptionField_by_fieldNames_array() {
        Map<String, Object> options = OpflowUtil.buildMap()
                .put("field1", "Hello world")
                .put("field2", 1024)
                .put("field3", OpflowUtil.buildMap()
                        .put("subfield1", "Java")
                        .put("subfield2", 3.14159)
                        .toMap())
                .toMap();
        Assert.assertNull(OpflowUtil.getOptionField(null, new String[] { "field1" }));
        Assert.assertNull(OpflowUtil.getOptionField(options, null));
        Assert.assertNull(OpflowUtil.getOptionField(options, new String[0]));
        Assert.assertEquals("Java", OpflowUtil.getOptionField(options, new String[] {"field3", "subfield1"}));
    }
}
