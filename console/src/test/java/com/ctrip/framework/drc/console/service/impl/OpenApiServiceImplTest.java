package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.vo.MhaGroupFilterVo;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;


public class OpenApiServiceImplTest {
    
    @Mock
    private MetaGenerator metaGenerator;
    
    @Mock
    private MetaInfoServiceImpl metaInfoService;
    
    @InjectMocks
    private OpenApiServiceImpl openApiService;

    

    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);

        List<MhaGroupTbl> mhaGroupTbls = Lists.newArrayList();
        List<GroupMappingTbl> groupMappingTbls = Lists.newArrayList();
        List<MhaTbl> mhaTbls = Lists.newArrayList();
        List<MachineTbl> machineTbls = Lists.newArrayList();
        List<DcTbl> dcTbls = Lists.newArrayList();

        MhaGroupTbl mhaGroupTbl1 = new MhaGroupTbl();
        GroupMappingTbl groupMappingTbl1_1 = new GroupMappingTbl();
        GroupMappingTbl groupMappingTbl1_2 = new GroupMappingTbl();
        MhaTbl mhaTbl1 = new MhaTbl();
        MhaTbl mhaTbl2 = new MhaTbl();
        MachineTbl machine1 = new MachineTbl();
        MachineTbl machine2 = new MachineTbl();
        DcTbl dcTbl1 = new DcTbl();
        DcTbl dcTbl2 = new DcTbl();
        
        mhaGroupTbl1.setId(1L);
        mhaGroupTbl1.setDrcEstablishStatus(EstablishStatusEnum.ESTABLISHED.getCode());
        
        groupMappingTbl1_1.setId(1L);
        groupMappingTbl1_1.setMhaGroupId(1L);
        groupMappingTbl1_1.setMhaId(1L);
        
        groupMappingTbl1_2.setId(2L);
        groupMappingTbl1_2.setMhaGroupId(1L);
        groupMappingTbl1_2.setMhaId(2L);
        
        mhaTbl1.setId(1L);
        mhaTbl1.setMhaName("mha1");
        mhaTbl1.setDcId(1L);
        
        mhaTbl2.setId(2L);
        mhaTbl2.setMhaName("mha2");
        mhaTbl2.setDcId(2L);
        
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
        
        
        
        MhaGroupTbl mhaGroupTbl2 = new MhaGroupTbl();
        GroupMappingTbl groupMappingTbl2_3 = new GroupMappingTbl();
        GroupMappingTbl groupMappingTbl2_4 = new GroupMappingTbl();
        MhaTbl mhaTbl3 = new MhaTbl();
        MhaTbl mhaTbl4 = new MhaTbl();
        MachineTbl machine3 = new MachineTbl();
        MachineTbl machine4 = new MachineTbl();


        mhaGroupTbl2.setId(2L);
        mhaGroupTbl2.setDrcEstablishStatus(EstablishStatusEnum.ESTABLISHED.getCode());

        groupMappingTbl2_3.setId(3L);
        groupMappingTbl2_3.setMhaGroupId(2L);
        groupMappingTbl2_3.setMhaId(3L);

        groupMappingTbl2_4.setId(4L);
        groupMappingTbl2_4.setMhaGroupId(2L);
        groupMappingTbl2_4.setMhaId(4L);

        mhaTbl3.setId(3L);
        mhaTbl3.setMhaName("mha3");
        mhaTbl3.setDcId(1L);

        mhaTbl4.setId(4L);
        mhaTbl4.setMhaName("mha4");
        mhaTbl4.setDcId(2L);

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
        
        mhaGroupTbls.add(mhaGroupTbl1);
        mhaGroupTbls.add(mhaGroupTbl2);
        groupMappingTbls.add(groupMappingTbl1_1);
        groupMappingTbls.add(groupMappingTbl1_2);
        groupMappingTbls.add(groupMappingTbl2_3);
        groupMappingTbls.add(groupMappingTbl2_4);
        mhaTbls.add(mhaTbl1);
        mhaTbls.add(mhaTbl2);
        mhaTbls.add(mhaTbl3);
        mhaTbls.add(mhaTbl4);
        machineTbls.add(machine1);
        machineTbls.add(machine2);
        machineTbls.add(machine3);
        machineTbls.add(machine4);
        dcTbls.add(dcTbl1);
        dcTbls.add(dcTbl2);

        Mockito.doReturn(mhaGroupTbls).when(metaGenerator).getMhaGroupTbls();
        Mockito.doReturn(groupMappingTbls).when(metaGenerator).getGroupMappingTbls();
        Mockito.doReturn(mhaTbls).when(metaGenerator).getMhaTbls();
        Mockito.doReturn(machineTbls).when(metaGenerator).getMachineTbls();
        Mockito.doReturn(dcTbls).when(metaGenerator).getDcTbls();
        
        Mockito.doReturn(0).when(metaInfoService).getApplyMode("mha1","mha2");
        Mockito.doReturn("db1").when(metaInfoService).getIncludedDbs("mha1","mha2");
        Mockito.doReturn(0).when(metaInfoService).getApplyMode("mha2","mha1");
        Mockito.doReturn(null).when(metaInfoService).getIncludedDbs("mha2","mha1");
        Mockito.doReturn(1).when(metaInfoService).getApplyMode("mha3","mha4");
        Mockito.doReturn(null).when(metaInfoService).getNameFilter("mha3","mha4");
        Mockito.doReturn(1).when(metaInfoService).getApplyMode("mha4","mha3");
        Mockito.doReturn("db1\\.table1").when(metaInfoService).getNameFilter("mha4","mha3");
        
      
        
    }
    
    @Test
    public void testGetAllDrcMhaDbFilters() throws SQLException {
        List<MhaGroupFilterVo> allDrcMhaDbFilters = openApiService.getAllDrcMhaDbFilters();
        Assert.assertEquals(2,allDrcMhaDbFilters.size());
        
    }

}