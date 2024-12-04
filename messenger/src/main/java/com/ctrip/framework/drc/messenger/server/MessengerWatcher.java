package com.ctrip.framework.drc.messenger.server;

import com.ctrip.framework.drc.messenger.activity.monitor.WatchActivity;
import com.ctrip.framework.drc.messenger.container.MqServerContainer;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import com.ctrip.framework.drc.fetcher.server.FetcherServer;
import com.ctrip.framework.drc.fetcher.system.AbstractLink;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author Slight
 * Aug 19, 2020
 */
public class MessengerWatcher extends AbstractLink {

    public MessengerWatcher(MqServerContainer container, ConcurrentHashMap<String, ? extends FetcherServer> servers) throws Exception {
        setConfig(new WatcherConfig(container, servers), WatcherConfig.class);
        setName("MessengerWatcher");

        source(WatchActivity.class)
                .with(ExecutorResource.class);
    }

    public static class WatcherConfig {

        private ConcurrentHashMap<String, ? extends FetcherServer> servers;

        private MqServerContainer container;

        public WatcherConfig(MqServerContainer container, ConcurrentHashMap<String, ? extends FetcherServer> servers) {
            this.container = container;
            this.servers = servers;
        }

        public ConcurrentHashMap<String, ? extends FetcherServer> getServers() {
            return servers;
        }

        public MqServerContainer getContainer() {
            return container;
        }

        public int getMaxThreads() {
            return 1;
        }

        public int getCoreThreads() {
            return 1;
        }
    }
}
