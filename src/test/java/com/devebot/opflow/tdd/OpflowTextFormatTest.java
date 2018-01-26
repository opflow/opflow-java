package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.supports.OpflowTextFormat;
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
public class OpflowTextFormatTest {

    @Test
    public void test_fill_template_with_MapObject() {
        String expected = "Hi all, my name is Joe Doe, I'm 22 year olds, and I'm working for Opflow.";
        String output = OpflowTextFormat.format(
            "Hi all, my name is ${profile.name}, I'm ${profile.age} year olds, and I'm working for ${profile.company}.", 
            OpflowUtil.buildMap()
                    .put("profile", OpflowUtil.buildMap()
                            .put("name", "Joe Doe")
                            .put("age", 22)
                            .put("company", "Opflow")
                            .toMap())
                    .toMap()
        );
        System.out.println("Output: " + output);
        Assert.assertEquals(expected, output);
    }
}
