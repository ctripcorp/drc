package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.vo.api.DrcDbInfo;
import com.ctrip.framework.drc.console.vo.api.MessengerInfo;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.FileUtils;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;

import static com.ctrip.framework.drc.core.service.utils.Constants.ESCAPE_CHARACTER_DOT_REGEX;


public class OpenApiServiceImplTest {

    @Mock
    private MetaProviderV2 metaProviderV2;

    @InjectMocks
    private OpenApiServiceImpl openApiService;


    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);

        List<MachineTbl> machineTbls = Lists.newArrayList();
        List<DcTbl> dcTbls = Lists.newArrayList();

        MachineTbl machine1 = new MachineTbl();
        MachineTbl machine2 = new MachineTbl();
        DcTbl dcTbl1 = new DcTbl();
        DcTbl dcTbl2 = new DcTbl();

        machine1.setId(1L);
        machine1.setMhaId(1L);
        machine1.setIp("ip");
        machine1.setIp("port");
        machine1.setMaster(BooleanEnum.TRUE.getCode());

        machine2.setId(2L);
        machine2.setMhaId(2L);
        machine2.setIp("ip");
        machine2.setIp("port");
        machine2.setMaster(BooleanEnum.TRUE.getCode());

        dcTbl1.setId(1L);
        dcTbl1.setDcName("dc1");

        dcTbl2.setId(2L);
        dcTbl2.setDcName("dc2");

        MachineTbl machine3 = new MachineTbl();
        MachineTbl machine4 = new MachineTbl();



        machine3.setId(3L);
        machine3.setMhaId(3L);
        machine3.setIp("ip");
        machine3.setIp("port");
        machine3.setMaster(BooleanEnum.TRUE.getCode());

        machine4.setId(4L);
        machine4.setMhaId(4L);
        machine4.setIp("ip");
        machine4.setIp("port");
        machine4.setMaster(BooleanEnum.TRUE.getCode());

        machineTbls.add(machine1);
        machineTbls.add(machine2);
        machineTbls.add(machine3);
        machineTbls.add(machine4);
        dcTbls.add(dcTbl1);
        dcTbls.add(dcTbl2);
    }


    @Test
    public void testGetAllDrcDbInfo() throws Exception {
        Drc drc = DefaultSaxParser.parse(FileUtils.getFileInputStream("api/open_api_meta.xml"));
        Mockito.doReturn(drc).when(metaProviderV2).getDrc();

        List<DrcDbInfo> allDrcDbInfo = openApiService.getDrcDbInfos(null);
        Assert.assertNotEquals(0, allDrcDbInfo.size());
    }


    @Test
    public void testGetAllMessengersInfo() throws Exception {
        Drc drc = DefaultSaxParser.parse(FileUtils.getFileInputStream("api/open_api_meta.xml"));
        Mockito.doReturn(drc).when(metaProviderV2).getDrc();

        List<MessengerInfo> allMessengersInfo = openApiService.getAllMessengersInfo();
        Assert.assertEquals(1, allMessengersInfo.size());
    }

    @Test
    public void testSplit() {
        String nameFilter = "drc\\d\\..*";  ///    drc\d\..*
        String[] split = nameFilter.split("\\\\.");  //       \\.
        String[] split1 = nameFilter.split(ESCAPE_CHARACTER_DOT_REGEX);  //     \\\.
        Assert.assertEquals(3, split.length);
        Assert.assertEquals(2, split1.length);
    }

}