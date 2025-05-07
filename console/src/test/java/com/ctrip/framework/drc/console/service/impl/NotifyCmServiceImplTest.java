package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.DlockEnum;
import com.ctrip.framework.drc.console.enums.HttpRequestEnum;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

/**
 * Created by shiruixin
 * 2025/4/16 17:46
 */
public class NotifyCmServiceImplTest {
    @InjectMocks
    private NotifyCmServiceImpl notifyCmService;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private DcTblDao dcTblDao;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setDcId(1L);
        mhaTblV2.setClusterName("cluster");
        mhaTblV2.setMhaName("mha");
        DcTbl dcTbl = new DcTbl();
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("mha"), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mhaTblV2);
        Mockito.when(dcTblDao.queryById(Mockito.eq(1L))).thenReturn(dcTbl);
    }

    @Test
    public void testPushConfigToCM() throws Exception {
        notifyCmService.pushConfigToCM(List.of("mha"), DlockEnum.AUTOCONFIG, HttpRequestEnum.PUT);
        notifyCmService.pushConfigToCM(List.of("mha"), DlockEnum.AUTOCONFIG, HttpRequestEnum.POST);
    }
}