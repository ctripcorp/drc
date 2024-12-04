package com.ctrip.framework.drc.messenger.server;

import com.ctrip.framework.drc.fetcher.system.Activity;
import com.ctrip.framework.drc.fetcher.system.Resource;
import org.junit.Test;

import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

/**
 * Created by shiruixin
 * 2024/11/8 16:56
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
        assertTrue(resources.containsKey("Executor"));
        assertTrue(resources.containsKey("LinkContext"));
        assertTrue(resources.containsKey("LWM"));
        assertTrue(resources.containsKey("Progress"));
        assertTrue(resources.containsKey("Capacity"));
        assertTrue(resources.containsKey("ListenableDirectMemory"));
        assertTrue(resources.containsKey("MqProvider"));
        assertTrue(resources.containsKey("MqPosition"));

        Map<String, Activity> activities = server.getActivities();
        assertTrue(activities.containsKey("MqApplierDumpEventActivity"));
        assertTrue(activities.containsKey("MqMetricsActivity"));
        assertTrue(activities.containsKey("LoadEventActivity"));
        assertTrue(activities.containsKey("InvolveActivity"));
        assertTrue(activities.containsKey("ApplierGroupActivity"));
        assertTrue(activities.containsKey("DispatchActivity"));
        assertTrue(activities.containsKey("CommitActivity"));

        //One important this is not tested that activities must be defined
        //as an unit of ApplierServer.
    }
}