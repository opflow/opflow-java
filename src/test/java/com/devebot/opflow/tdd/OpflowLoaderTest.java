package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowLoader;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowBootstrapException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.yaml.snakeyaml.parser.ParserException;
import org.yaml.snakeyaml.scanner.ScannerException;

/**
 *
 * @author drupalex
 */
public class OpflowLoaderTest {
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testYamlLoading() throws OpflowBootstrapException {
        Map<String,Object> loaded = OpflowLoader.loadConfiguration("opflow.yml");
        System.out.println("JSON: " + OpflowUtil.jsonMapToString(loaded));
    }
    
    @Test
    public void testYamlLoadingNotFound() throws OpflowBootstrapException {
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectCause(CoreMatchers.is(FileNotFoundException.class));
        thrown.expectMessage(CoreMatchers.is("java.io.FileNotFoundException: configuration file 'opflow_notfound.yml' not found"));
        Map<String,Object> loaded = OpflowLoader.loadConfiguration("opflow_notfound.yml");
        System.out.println("JSON: " + OpflowUtil.jsonMapToString(loaded));
    }
    
    @Test
    public void testYamlLoadingThrowParserException() throws OpflowBootstrapException {
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectCause(CoreMatchers.is(ParserException.class));
        thrown.expectMessage(CoreMatchers.startsWith("expected '<document start>', but found BlockMappingStart\n in 'reader', line"));
        Map<String,Object> loaded = OpflowLoader.loadConfiguration("opflow_invalid1.yml");
        System.out.println("JSON: " + OpflowUtil.jsonMapToString(loaded));
    }
    
    @Test
    public void testYamlLoadingThrowScannerException() throws OpflowBootstrapException {
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectCause(CoreMatchers.is(ScannerException.class));
        thrown.expectMessage(CoreMatchers.startsWith("mapping values are not allowed here\n in 'reader', line"));
        Map<String,Object> loaded = OpflowLoader.loadConfiguration("opflow_invalid2.yml");
        System.out.println("JSON: " + OpflowUtil.jsonMapToString(loaded));
    }
    
    @Test
    public void testGetConfigurationExtension() throws MalformedURLException {
        URL url = new URL("http://example.com/opflow.properties");
        assertThat(OpflowLoader.getConfigurationExtension(url), equalTo("properties"));
        
        url = new URL("file:///home/ubuntu/opflow.yml");
        assertThat(OpflowLoader.getConfigurationExtension(url), equalTo("yml"));
    }
}
