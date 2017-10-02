package com.devebot.opflow.bdd;

import com.devebot.opflow.bdd.steps.OpflowCommanderSteps;
import com.devebot.opflow.bdd.steps.OpflowCommonSteps;
import com.devebot.opflow.bdd.steps.OpflowServerletSteps;
import java.util.Arrays;
import java.util.List;
import org.jbehave.core.io.CodeLocations;
import org.jbehave.core.io.StoryFinder;
import org.jbehave.core.steps.InstanceStepsFactory;
import org.jbehave.core.steps.InjectableStepsFactory;

/**
 *
 * @author drupalex
 */
public class OpflowServerletStories extends OpflowEmbedder {

    @Override
    public InjectableStepsFactory stepsFactory() {
        return new InstanceStepsFactory(configuration(),
                new OpflowCommonSteps(),
                new OpflowCommanderSteps(),
                new OpflowServerletSteps());
    }
    
    @Override
    protected List<String> storyPaths() {
        String codeLocation = CodeLocations.codeLocationFromClass(this.getClass()).getFile();
        return new StoryFinder().findPaths(codeLocation, Arrays.asList("**/serverlet-*.story"), Arrays.asList(""), "file:" + codeLocation);
    }
}
