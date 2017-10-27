package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.OpflowJsontool;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowBootstrapException;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.yaml.snakeyaml.parser.ParserException;
import org.yaml.snakeyaml.scanner.ScannerException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 * @author drupalex
 */
public class OpflowBuilderTest {
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void test_system_Property_has_highest_priority() throws OpflowBootstrapException {
        System.setProperty(OpflowBuilder.DEFAULT_CONFIGURATION_KEY, "opflow_mirror.yml");
        Map<String,Object> loaded = OpflowBuilder.loadConfiguration("opflow.yml");
        System.out.println("opflow_mirror.yml: " + OpflowJsontool.toString(loaded));
        assertThat(OpflowUtil.getOptionField(loaded, new String[] {
            "opflow", "worker", "exchangeName"
        }).toString(), equalTo("tdd-mirror-exchange"));
        System.clearProperty(OpflowBuilder.DEFAULT_CONFIGURATION_KEY);
    }
    
    @Test
    public void testYamlLoading() throws OpflowBootstrapException {
        Map<String,Object> loaded = OpflowBuilder.loadConfiguration("opflow.yml");
        System.out.println("opflow.yml: " + OpflowJsontool.toString(loaded));
        assertThat(OpflowUtil.getOptionField(loaded, new String[] {
            "opflow", "worker", "exchangeName"
        }).toString(), equalTo("tdd-opflow-exchange"));
    }
    
    @Test
    public void testYamlLoadingNotFound() throws OpflowBootstrapException {
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectCause(CoreMatchers.is(FileNotFoundException.class));
        thrown.expectMessage(CoreMatchers.is("java.io.FileNotFoundException: configuration file 'opflow_notfound.yml' not found"));
        Map<String,Object> loaded = OpflowBuilder.loadConfiguration("opflow_notfound.yml");
        System.out.println("JSON: " + OpflowJsontool.toString(loaded));
    }
    
    @Test
    public void testYamlLoadingThrowParserException() throws OpflowBootstrapException {
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectCause(CoreMatchers.is(ParserException.class));
        thrown.expectMessage(CoreMatchers.startsWith("expected '<document start>', but found BlockMappingStart\n in 'reader', line"));
        Map<String,Object> loaded = OpflowBuilder.loadConfiguration("opflow_invalid1.yml");
        System.out.println("JSON: " + OpflowJsontool.toString(loaded));
    }
    
    @Test
    public void testYamlLoadingThrowScannerException() throws OpflowBootstrapException {
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectCause(CoreMatchers.is(ScannerException.class));
        thrown.expectMessage(CoreMatchers.startsWith("mapping values are not allowed here\n in 'reader', line"));
        Map<String,Object> loaded = OpflowBuilder.loadConfiguration("opflow_invalid2.yml");
        System.out.println("JSON: " + OpflowJsontool.toString(loaded));
    }
    
    @Test
    public void testGetConfigurationExtension() throws MalformedURLException {
        URL url = new URL("http://example.com/opflow.properties");
        assertThat(OpflowBuilder.getConfigurationExtension(url), equalTo("properties"));
        
        url = new URL("file:///home/ubuntu/opflow.yml");
        assertThat(OpflowBuilder.getConfigurationExtension(url), equalTo("yml"));
    }
}
