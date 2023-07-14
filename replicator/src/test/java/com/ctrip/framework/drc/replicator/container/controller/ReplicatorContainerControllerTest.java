package com.ctrip.framework.drc.replicator.container.controller;

import com.ctrip.framework.drc.core.driver.config.GlobalConfig;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorConfigDto;
import com.ctrip.framework.drc.core.server.container.ServerContainer;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.List;

import static com.ctrip.framework.drc.replicator.AllTests.*;
import static com.ctrip.framework.drc.replicator.AllTests.MYSQL_PASSWORD;

/**
 * Created by jixinwang on 2023/6/25
 */
public class ReplicatorContainerControllerTest {

    public static final String BU = "BBZ";

    public static final String MHA_NAME = "testOyMhaName";

    public static final String CLUSTER_NAME = "test_clusterName";

    public static final String UUID = "80ec424f-faeb-11e9-922d-fa163eb4df21";

    public static final Long APPID = 1222l;

    public static final int APPLIER_PORT = 8383;

    private ReplicatorConfigDto configDto = new ReplicatorConfigDto();

    private ReplicatorConfig replicatorConfig;

    @Mock
    private ServerContainer<ReplicatorConfig, ApiResult> serverContainer;

    @InjectMocks
    private ReplicatorContainerController controller = new ReplicatorContainerController();

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
        configDto.setBu(BU);
        configDto.setClusterAppId(APPID);
        configDto.setMhaName(MHA_NAME);
        configDto.setSrcDcName(System.getProperty(DefaultFoundationService.DATA_CENTER_KEY, GlobalConfig.DC));

        configDto.setStatus(InstanceStatus.ACTIVE.getStatus());
        configDto.setClusterName(CLUSTER_NAME);
        configDto.setGtidSet("");
        configDto.setApplierPort(APPLIER_PORT);

        List<String> uuids = Lists.newArrayList();
        Db db = new Db();
        db.setMaster(true);
        db.setPort(SRC_PORT);
        db.setIp(SRC_IP);
        db.setUuid(UUID);

        uuids.add(UUID);
        configDto.setReadUser(MYSQL_USER);
        configDto.setReadPassward(MYSQL_PASSWORD);
        configDto.setUuids(uuids);
        configDto.setPreviousMaster("");
        configDto.setMaster(db);

        replicatorConfig = configDto.toReplicatorConfig();
    }

    @Test
    public void start() throws InterruptedException {
        controller.start(configDto);
        Thread.sleep(200);
        Mockito.when(serverContainer.getUpstreamMaster(replicatorConfig.getRegistryKey())).thenReturn(replicatorConfig.getEndpoint());
        controller.start(configDto);
        Thread.sleep(200);
        Mockito.verify(serverContainer, Mockito.times(1)).removeServer(Mockito.anyString(), Mockito.anyBoolean());
        Mockito.verify(serverContainer, Mockito.times(1)).addServer(Mockito.any());
    }

    @Test
    public void register() throws InterruptedException {
        controller.register(configDto);
        Thread.sleep(200);
        Mockito.verify(serverContainer, Mockito.times(1)).register(Mockito.anyString(), Mockito.anyInt());
    }

    @Test
    public void delete() throws InterruptedException {
        controller.destroy(replicatorConfig.getRegistryKey());
        Thread.sleep(200);
        Mockito.verify(serverContainer, Mockito.times(1)).removeServer(Mockito.anyString(), Mockito.anyBoolean());
    }
}