package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.v2.DlockTblDao;
import com.ctrip.framework.drc.console.enums.DlockEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by shiruixin
 * 2025/4/23 15:29
 */
public class DlockServiceImplTest {
    @InjectMocks
    DlockServiceImpl dLockServiceImpl;
    @Mock
    private DlockTblDao dlockTblDao;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testTryLocks() throws SQLException {
        Mockito.when(dlockTblDao.deleteByMhaNames(Mockito.any(), Mockito.anyInt())).thenReturn(1);
        Mockito.when(dlockTblDao.batchInsert(Mockito.any())).thenReturn(null);
        dLockServiceImpl.tryLocks(List.of("mhaName"), DlockEnum.AUTOCONFIG);
    }

    @Test(expected = ConsoleException.class)
    public void testTryLocksFail() throws SQLException {
        Mockito.when(dlockTblDao.deleteByMhaNames(Mockito.any(), Mockito.anyInt())).thenReturn(1);
        Mockito.when(dlockTblDao.batchInsert(Mockito.any())).thenThrow(new SQLException(""));
        dLockServiceImpl.tryLocks(List.of("mhaName"),  DlockEnum.AUTOCONFIG);
    }

    @Test(expected = ConsoleException.class)
    public void testUnlockLocksFail() throws SQLException {
        Mockito.when(dlockTblDao.deleteByMhaNames(Mockito.any())).thenThrow(new SQLException(""));
        dLockServiceImpl.unlockLocks(List.of("mhaName"),  DlockEnum.AUTOCONFIG);
    }
}