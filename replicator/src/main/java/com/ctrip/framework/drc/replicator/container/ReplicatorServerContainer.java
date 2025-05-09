package com.ctrip.framework.drc.replicator.container;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.container.ServerContainer;
import com.ctrip.framework.drc.replicator.ReplicatorServer;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.DefaultReplicatorServer;
import com.ctrip.framework.drc.replicator.container.config.TableFilterConfiguration;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.SchemaManagerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Created by mingdongli
 * 2019/10/30 上午9:14.
 */
@Component
public class ReplicatorServerContainer extends AbstractServerContainer implements ServerContainer<ReplicatorConfig, ApiResult>, ApplicationRunner {

    @Autowired
    private UuidOperator uuidOperator;

    @Autowired
    private TableFilterConfiguration tableFilterConfiguration;

    @Autowired
    public void setApplicationContext(ApplicationContext context) {
        this.context = context;
    }

    @Override
    protected ReplicatorServer getReplicatorServer(ReplicatorConfig config) {
        return new DefaultReplicatorServer(config, SchemaManagerFactory.getOrCreateMySQLSchemaManager(config), tableFilterConfiguration, uuidOperator);
    }

}
