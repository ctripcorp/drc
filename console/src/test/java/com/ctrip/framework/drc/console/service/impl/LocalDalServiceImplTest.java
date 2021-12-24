package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.core.service.dal.DalClusterTypeEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-28
 */
public class LocalDalServiceImplTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private DalServiceImpl dalService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        Mockito.when(monitorTableSourceProvider.getDalServiceTimeout()).thenReturn(2000);
    }

    @Test
    public void testSwitchDalClusterType() {
        List<String> types = Arrays.asList(DalClusterTypeEnum.DRC.getValue(), DalClusterTypeEnum.NORMAL.getValue());
        Set<String> dalClusterNames = Sets.newHashSet("bbzdrcbenchmarkdb_dalcluster", "bbzdrccameldb_dalcluster");

        // switch to drc
        dalClusterNames.forEach(dalClusterName -> {
            try {
                ApiResult result = dalService.switchDalClusterType(dalClusterName, "fat", DalClusterTypeEnum.DRC, null);
                logger.info("{} to drc status: {}", dalClusterName, result.getStatus());
                logger.info("{} to drc message: {}", dalClusterName, result.getMessage());
            } catch (Exception e) {
                logger.error("Fail switch drc ", e);
            }
        });
        // check
        List<DalServiceImpl.DalClusterInfoWrapper> wrappers = dalService.getDalClusterInfoWrappers(dalClusterNames, "fat");
        Assert.assertNotNull(wrappers);
        System.out.println(wrappers);
        wrappers.forEach(wrapper -> {
            Assert.assertTrue(dalClusterNames.contains(wrapper.getDalClusterName()));
            Assert.assertTrue(DalClusterTypeEnum.DRC.getValue().equalsIgnoreCase(wrapper.getType()));
            Assert.assertNotEquals(0, wrapper.getZoneIds().size());
        });

        // switch to ntgxh
        dalClusterNames.forEach(dalClusterName -> {
            try {
                ApiResult result = dalService.switchDalClusterType(dalClusterName, "fat", DalClusterTypeEnum.NORMAL, "ntgxh");
                logger.info("{} to ntgxh status: {}", dalClusterName, result.getStatus());
                logger.info("{} to ntgxh message: {}", dalClusterName, result.getMessage());
            } catch (Exception e) {
                logger.error("Fail failover to ntgxh ", e);
            }
        });
        // check
        wrappers = dalService.getDalClusterInfoWrappers(dalClusterNames, "fat");
        Assert.assertNotNull(wrappers);
        System.out.println(wrappers);
        wrappers.forEach(wrapper -> {
            Assert.assertTrue(dalClusterNames.contains(wrapper.getDalClusterName()));
            Assert.assertTrue(DalClusterTypeEnum.NORMAL.getValue().equalsIgnoreCase(wrapper.getType()));
            Assert.assertNotEquals(0, wrapper.getZoneIds().size());
        });

        // switch to ntgxh again
        dalClusterNames.forEach(dalClusterName -> {
            try {
                ApiResult result = dalService.switchDalClusterType(dalClusterName, "fat", DalClusterTypeEnum.NORMAL, "ntgxh");
                logger.info("{} to ntgxh again status: {}", dalClusterName, result.getStatus());
                logger.info("{} to ntgxh again message: {}", dalClusterName, result.getMessage());
            } catch (Exception e) {
                logger.error("Fail failover to ntgxh ", e);
            }
        });
        // check
        wrappers = dalService.getDalClusterInfoWrappers(dalClusterNames, "fat");
        Assert.assertNotNull(wrappers);
        System.out.println(wrappers);
        wrappers.forEach(wrapper -> {
            Assert.assertTrue(dalClusterNames.contains(wrapper.getDalClusterName()));
            Assert.assertTrue(DalClusterTypeEnum.NORMAL.getValue().equalsIgnoreCase(wrapper.getType()));
            Assert.assertNotEquals(0, wrapper.getZoneIds().size());
        });

        // switch to stgxh
        dalClusterNames.forEach(dalClusterName -> {
            try {
                ApiResult result = dalService.switchDalClusterType(dalClusterName, "fat", DalClusterTypeEnum.NORMAL, "stgxh");
                logger.info("{} to stgxh status: {}", dalClusterName, result.getStatus());
                logger.info("{} to stgxh again message: {}", dalClusterName, result.getMessage());
            } catch (Exception e) {
                logger.error("Fail failover to ntgxh ", e);
            }
        });
        // check
        wrappers = dalService.getDalClusterInfoWrappers(dalClusterNames, "fat");
        Assert.assertNotNull(wrappers);
        System.out.println(wrappers);
        wrappers.forEach(wrapper -> {
            Assert.assertTrue(dalClusterNames.contains(wrapper.getDalClusterName()));
            Assert.assertTrue(DalClusterTypeEnum.NORMAL.getValue().equalsIgnoreCase(wrapper.getType()));
            Assert.assertNotEquals(0, wrapper.getZoneIds().size());
        });
    }
}
