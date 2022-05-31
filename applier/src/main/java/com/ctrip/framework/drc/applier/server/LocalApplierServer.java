package com.ctrip.framework.drc.applier.server;

import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.COMMA;

/**
 * @Author Slight
 * Sep 18, 2019
 */
public class LocalApplierServer extends ApplierServer {

    public LocalApplierServer() throws Exception {
        this(3306, 8383, SystemConfig.INTEGRITY_TEST, Sets.newHashSet(), null);
    }

    public LocalApplierServer(int destMySQLPort, int replicatorPort, String destination, Set<String> includedDb, String properties) throws Exception {
        ApplierConfigDto config = createApplierDto(destMySQLPort, replicatorPort, destination, includedDb, properties);
        setConfig(config, ApplierConfigDto.class);
        setName(config.getRegistryKey());
        define();
    }

    public ApplierConfigDto createApplierDto(int destMySQLPort, int replicatorPort, String registryKey, Set<String> includedDb, String properties) {
        ApplierConfigDto config = new ApplierConfigDto();
        config.setGtidExecuted("");
        config.setIdc("dest");
        config.setCluster("cluster");
        config.setName("[" + replicatorPort + "]->LOCAL_APPLIER->[" + destMySQLPort + "]");
        config.setIncludedDbs(StringUtils.join(includedDb, COMMA));
        config.setProperties(properties);
        config.setApplyMode(ApplyMode.set_gtid.getType());

        InstanceInfo replicator = new InstanceInfo();
        replicator.setIdc("src");
        replicator.setIp("127.0.0.1");
        replicator.setPort(replicatorPort);
        replicator.setMhaName("mha2");

        DBInfo db = new DBInfo();
        db.setIp("127.0.0.1");
        db.setPort(destMySQLPort);
        db.setUsername("root");
        db.setPassword("root");
        db.setMhaName("mha1");

        config.setReplicator(replicator);
        config.setTarget(db);
        return config;
    }
}
