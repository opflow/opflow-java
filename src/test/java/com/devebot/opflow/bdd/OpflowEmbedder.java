package com.devebot.opflow.bdd;

import org.jbehave.core.configuration.Configuration;
import org.jbehave.core.configuration.MostUsefulConfiguration;
import org.jbehave.core.io.LoadFromURL;
import org.jbehave.core.junit.JUnitStories;
import org.jbehave.core.reporters.Format;
import org.jbehave.core.reporters.StoryReporterBuilder;

/**
 *
 * @author drupalex
 */
public abstract class OpflowEmbedder extends JUnitStories {
    @Override
    public Configuration configuration() {
        return new MostUsefulConfiguration()
                .useStoryLoader(new LoadFromURL())
                .useStoryReporterBuilder(new StoryReporterBuilder()
                        .withDefaultFormats()
			.withFormats(Format.CONSOLE, Format.TXT));
    }
}
