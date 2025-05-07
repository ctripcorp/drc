package com.ctrip.framework.drc.fetcher.server;

import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherConfigDto;
import com.ctrip.framework.drc.fetcher.activity.event.FetcherDumpEventActivity;
import com.ctrip.framework.drc.fetcher.resource.condition.LWMResource;
import com.ctrip.framework.drc.fetcher.resource.condition.ProgressResource;
import com.ctrip.framework.drc.fetcher.resource.position.TransactionTableResource;
import com.ctrip.framework.drc.fetcher.system.AbstractLink;

/**
 * @Author Slight
 * Oct 24, 2019
 */
public abstract class FetcherServer extends AbstractLink {
    public static final int DEFAULT_APPLY_COUNT = 10;

    public FetcherConfigDto config;

    public abstract void define() throws Exception;

    public FetcherServer() {}

    public FetcherServer(FetcherConfigDto config) throws Exception {
        this.config = config;
        parseConfig(config);
        setConfig(config, FetcherConfigDto.class);
        setName(config.getRegistryKey());
        define();
    }

    private void parseConfig(FetcherConfigDto config) throws Exception {
        String properties = config.getProperties();
        Integer concurrency = DataMediaConfig.getConcurrency(properties);

        int applyConcurrency = getApplyApplyConcurrency();

        if (concurrency != null && concurrency > 0 && concurrency <= 100) {
            applyConcurrency = concurrency;
        }
        config.setApplyConcurrency(applyConcurrency);
    }

    abstract protected int getApplyApplyConcurrency();

    public long getLWM() {
        return ((LWMResource) resources.get("LWM")).current();
    }

    public long getProgress() {
        return ((ProgressResource) resources.get("Progress")).get();
    }


    public TransactionTableResource getTransactionTableResource() {
        return ((TransactionTableResource) resources.get("TransactionTable"));
    }

    public abstract FetcherDumpEventActivity getDumpEventActivity();
}
