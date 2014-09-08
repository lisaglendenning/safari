package edu.uw.zookeeper.safari;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SingleRegionTest extends AbstractMainTest {

    @Test(timeout=40000)
    public void testStartAndStop() throws Exception {
        final long pause = 8000L;
        final List<Component<?>> components = SafariModules.newSafariRegion();
        pauseWithComponents(components, pause);
    }
}
