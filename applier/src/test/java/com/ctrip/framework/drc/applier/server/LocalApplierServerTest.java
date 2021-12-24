package com.ctrip.framework.drc.applier.server;

import com.ctrip.framework.drc.fetcher.system.Activity;
import com.ctrip.framework.drc.fetcher.system.Resource;
import org.junit.Test;

import java.util.Map;

import static junit.framework.TestCase.assertTrue;

/**
 * @Author Slight
 * Sep 25, 2019
 */
public class LocalApplierServerTest {

    @Test
    public void necessaryResource() throws Exception {
        class TestServer extends LocalApplierServer {
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
        assertTrue(resources.containsKey("LWM"));
        assertTrue(resources.containsKey("Executor"));
        assertTrue(resources.containsKey("LinkContext"));

        Map<String, Activity> activities = server.getActivities();
        assertTrue(activities.containsKey("ApplierDumpEventActivity"));
        assertTrue(activities.containsKey("InvolveActivity"));
        assertTrue(activities.containsKey("ApplierGroupActivity"));
        assertTrue(activities.containsKey("DispatchActivity"));

        //One important this is not tested that activities must be defined
        //as an unit of ApplierServer.
    }

}