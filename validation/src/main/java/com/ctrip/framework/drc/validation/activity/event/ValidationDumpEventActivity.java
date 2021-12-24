package com.ctrip.framework.drc.validation.activity.event;

import com.ctrip.framework.drc.fetcher.activity.event.DumpEventActivity;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.event.FetcherEvent;
import com.ctrip.framework.drc.validation.activity.replicator.converter.ValidationByteBufConverter;
import com.ctrip.framework.drc.validation.activity.replicator.driver.ValidationPooledConnector;

/**
 * @Author limingdong
 * @create 2021/3/4
 */
public class ValidationDumpEventActivity extends DumpEventActivity<FetcherEvent> {

    @Override
    protected FetcherSlaveServer getFetcherSlaveServer() {
        return new FetcherSlaveServer(config, new ValidationPooledConnector(config.getEndpoint()), new ValidationByteBufConverter());
    }
}
