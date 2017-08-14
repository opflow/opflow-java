package com.devebot.opflow.bdd;

import com.devebot.opflow.bdd.steps.OpflowCommonSteps;
import com.devebot.opflow.bdd.steps.OpflowRpcMasterSteps;
import com.devebot.opflow.bdd.steps.OpflowRpcWorkerSteps;
import java.util.Arrays;
import java.util.List;
import org.jbehave.core.configuration.Configuration;
import org.jbehave.core.configuration.MostUsefulConfiguration;
import org.jbehave.core.io.CodeLocations;
import org.jbehave.core.io.LoadFromURL;
import org.jbehave.core.io.StoryFinder;
import org.jbehave.core.steps.InstanceStepsFactory;
import org.jbehave.core.junit.JUnitStories;
import org.jbehave.core.reporters.Format;
import org.jbehave.core.reporters.StoryReporterBuilder;
import org.jbehave.core.steps.InjectableStepsFactory;

/**
 *
 * @author drupalex
 */
public class OpflowRpcStories extends JUnitStories {
 
    @Override
    public Configuration configuration() {
        return new MostUsefulConfiguration()
                .useStoryLoader(new LoadFromURL())
                .useStoryReporterBuilder(new StoryReporterBuilder()
                        .withDefaultFormats()
			.withFormats(Format.CONSOLE, Format.TXT));
    }
 
    @Override
    public InjectableStepsFactory stepsFactory() {
        return new InstanceStepsFactory(configuration(),
                new OpflowCommonSteps(),
                new OpflowRpcMasterSteps(),
                new OpflowRpcWorkerSteps());
    }
     
    @Override
    protected List<String> storyPaths() {
        String codeLocation = CodeLocations.codeLocationFromClass(this.getClass()).getFile();
        return new StoryFinder().findPaths(codeLocation, Arrays.asList("**/*.story"), Arrays.asList(""), "file:" + codeLocation);
    }
}
