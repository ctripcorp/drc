package com.ctrip.framework.drc.console.service.checker;

import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by jixinwang on 2020/11/15
 */
public class ConflictLogCheckerTest {
    @InjectMocks
    private ConflictLogChecker conflictLogChecker;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testInBlackList() {
        Set<String> blackListSet = new HashSet<>();
        blackListSet.add("testCluster");
        Mockito.when(monitorTableSourceProvider.getConflictBlackList()).thenReturn(blackListSet);
        boolean isInBlackList = conflictLogChecker.inBlackList("testCluster");
        Assert.assertEquals(true, isInBlackList);
    }
}
