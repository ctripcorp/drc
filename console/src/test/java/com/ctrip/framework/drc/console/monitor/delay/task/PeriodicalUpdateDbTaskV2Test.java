package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dto.v3.MhaDbDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

public class PeriodicalUpdateDbTaskV2Test {
    @Mock
    Logger logger;
    @Mock
    DataCenterService dataCenterService;
    @Mock
    MonitorTableSourceProvider monitorTableSourceProvider;
    @Mock
    DefaultCurrentMetaManager currentMetaManager;
    @Mock
    DefaultConsoleConfig consoleConfig;
    @Mock
    CentralService centralService;
    @InjectMocks
    PeriodicalUpdateDbTaskV2 periodicalUpdateDbTaskV2;

    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        when(consoleConfig.getDcsInLocalRegion()).thenReturn(Sets.newHashSet("ntgxh"));
        when(consoleConfig.getDbApplierConfigureSwitch(anyString())).thenReturn(true);
        MhaDbReplicationDto dto = new MhaDbReplicationDto();
        dto.setSrc(new MhaDbDto(1L,"mha_db_1_src","db1"));
        dto.setDst(new MhaDbDto(1L,"mha_db_1_dst","db1"));
        dto.setReplicationType(ReplicationTypeEnum.DB_TO_DB.getType());
        dto.setDrcStatus(true);
        when(centralService.getMhaDbReplications(anyString())).thenReturn(Lists.newArrayList(dto));
        periodicalUpdateDbTaskV2.initialize();
    }

    @Test
    public void test() {
        Map<String, Set<String>> mhaDb1Dst = periodicalUpdateDbTaskV2.getMhaDbRelatedByDestMha("some_mha");
        Assert.assertTrue(mhaDb1Dst.isEmpty());

        Map<String, Set<String>> mhaDb2 = periodicalUpdateDbTaskV2.getMhaDbRelatedByDestMha("mha_db_1_dst");
        Assert.assertFalse(mhaDb2.isEmpty());
    }
}

