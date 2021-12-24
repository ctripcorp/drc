package com.ctrip.framework.drc.validation.server;

import com.ctrip.framework.drc.fetcher.system.Activity;
import com.ctrip.framework.drc.fetcher.system.Resource;
import org.junit.Test;

import java.util.Map;

import static junit.framework.TestCase.assertTrue;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/27
 */
public class LocalValidationServerTest {

    @Test
    public void necessaryResource() throws Exception {
        class TestServer extends LocalValidationServer {
            public TestServer() throws Exception {
            }

            public Map<String, Activity> getActivities() {
                return activities;
            }

            public Map<String, Resource> getResources() {
                return resources;
            }
        }
        TestServer server = new TestServer();

        Map<String, Resource> resources = server.getResources();
        assertTrue(resources.containsKey("Executor"));
        assertTrue(resources.containsKey("LinkContext"));

        Map<String, Activity> activities = server.getActivities();
        assertTrue(activities.containsKey("ValidationDumpEventActivity"));
        assertTrue(activities.containsKey("ValidationMetricsActivity"));
        assertTrue(activities.containsKey("ValidationResultActivity"));
        assertTrue(activities.containsKey("LoadEventActivity"));
        assertTrue(activities.containsKey("InvolveActivity"));
        assertTrue(activities.containsKey("ValidationGroupActivity"));
    }
}
