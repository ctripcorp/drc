package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v3.DbReplicationDto;
import com.ctrip.framework.drc.console.dto.v3.LogicTableConfig;
import com.ctrip.framework.drc.console.dto.v3.MhaDbDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.api.DrcDbInfo;
import com.ctrip.framework.drc.console.vo.api.MessengerInfo;
import com.ctrip.framework.drc.console.vo.api.RegionInfo;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.api.codec.Codec;
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
import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.core.service.utils.Constants.ESCAPE_CHARACTER_DOT_REGEX;


public class OpenApiServiceImplTest {

    @Mock
    private MetaProviderV2 metaProviderV2;

    @Mock
    private MysqlServiceV2 mysqlServiceV2;

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

        String expected = "[{\"db\":\"drc\\\\d\",\"table\":\".*\",\"srcMha\":\"fat-fx-drc2\",\"destMha\":\"fat-fx-drc1\",\"srcRegion\":\"ntgxy\",\"destRegion\":\"ntgxh\",\"rowsFilterConfigs\":[],\"columnsFilterConfigs\":[]},{\"db\":\"bbzbbzdrcbenchmarktmpdb\",\"table\":\"benchmark\",\"srcMha\":\"fat-fx-drc2\",\"destMha\":\"fat-fx-drc1\",\"srcRegion\":\"ntgxy\",\"destRegion\":\"ntgxh\",\"rowsFilterConfigs\":[{\"mode\":\"trip_udl\",\"tables\":\"bbzbbzdrcbenchmarktmpdb\\\\.row_filter\",\"configs\":{\"parameterList\":[{\"columns\":[\"uid\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}}],\"columnsFilterConfigs\":[]},{\"db\":\"drc\\\\d\",\"table\":\".*\",\"srcMha\":\"fat-fx-drc1\",\"destMha\":\"fat-fx-drc2\",\"srcRegion\":\"ntgxh\",\"destRegion\":\"ntgxy\",\"rowsFilterConfigs\":[],\"columnsFilterConfigs\":[]},{\"db\":\"bbzbbzdrcbenchmarktmpdb\",\"table\":\"benchmark\",\"srcMha\":\"fat-fx-drc1\",\"destMha\":\"fat-fx-drc2\",\"srcRegion\":\"ntgxh\",\"destRegion\":\"ntgxy\",\"rowsFilterConfigs\":[{\"mode\":\"trip_udl\",\"tables\":\"bbzbbzdrcbenchmarktmpdb\\\\.row_filter\",\"configs\":{\"parameterList\":[{\"columns\":[\"uid\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}}],\"columnsFilterConfigs\":[]}]";
        Assert.assertEquals(expected, Codec.DEFAULT.encode(allDrcDbInfo));
    }

    @Test
    public void testGetDrcDbInfoForDb() throws Exception {
        Drc drc = DefaultSaxParser.parse(FileUtils.getFileInputStream("api/open_api_meta.xml"));
        Mockito.doReturn(drc).when(metaProviderV2).getDrc();

        List<DrcDbInfo> dbInfos = openApiService.getDrcDbInfos("bbzbbzdrcbenchmarktmpdb");
        String expected = "[{\"db\":\"bbzbbzdrcbenchmarktmpdb\",\"table\":\"benchmark\",\"srcMha\":\"fat-fx-drc2\",\"destMha\":\"fat-fx-drc1\",\"srcRegion\":\"ntgxy\",\"destRegion\":\"ntgxh\",\"rowsFilterConfigs\":[{\"mode\":\"trip_udl\",\"tables\":\"bbzbbzdrcbenchmarktmpdb\\\\.row_filter\",\"configs\":{\"parameterList\":[{\"columns\":[\"uid\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}}],\"columnsFilterConfigs\":[]},{\"db\":\"bbzbbzdrcbenchmarktmpdb\",\"table\":\"benchmark\",\"srcMha\":\"fat-fx-drc1\",\"destMha\":\"fat-fx-drc2\",\"srcRegion\":\"ntgxh\",\"destRegion\":\"ntgxy\",\"rowsFilterConfigs\":[{\"mode\":\"trip_udl\",\"tables\":\"bbzbbzdrcbenchmarktmpdb\\\\.row_filter\",\"configs\":{\"parameterList\":[{\"columns\":[\"uid\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}}],\"columnsFilterConfigs\":[]}]";
        Assert.assertEquals(expected, Codec.DEFAULT.encode(dbInfos));
        List<DrcDbInfo> dbInfos2 = openApiService.getDrcDbInfos("notExist");
        Assert.assertEquals(0, dbInfos2.size());

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

    @Test
    public void testFilterAndConvert() {
        String db = "valid_db";

        List<MhaDbReplicationDto> list = new ArrayList<>();
        list.add(getMhaDbReplicationDto("sha", "sgp", true, Lists.newArrayList("table1", "(table2|table3)", "shard_\\d+")));
        list.add(getMhaDbReplicationDto("sgp", "fra", true, Lists.newArrayList("(test_fra_sha|table1)")));
        list.add(getMhaDbReplicationDto("sgp", "sha", false, Lists.newArrayList("(test_fra_sha|table1)")));


        // test check logic table
        // all
        List<RegionInfo> expectAll = Lists.newArrayList(new RegionInfo("sha", "sgp"), new RegionInfo("sgp", "fra"));
        Assert.assertEquals(expectAll, openApiService.filterAndConvert(list, db, "table1"));

        // sha -> sgp
        ArrayList<RegionInfo> expectSha = Lists.newArrayList(new RegionInfo("sha", "sgp"));
        Assert.assertEquals(expectSha, openApiService.filterAndConvert(list, db, "table2"));
        Assert.assertEquals(expectSha, openApiService.filterAndConvert(list, db, "table3"));
        Assert.assertEquals(expectSha, openApiService.filterAndConvert(list, db, "shard_01"));

        // sgp -> fra
        ArrayList<RegionInfo> expectSgpFra = Lists.newArrayList(new RegionInfo("sgp", "fra"));
        Assert.assertEquals(expectSgpFra, openApiService.filterAndConvert(list, db, "test_fra_sha"));

        // empty
        Assert.assertEquals(Lists.newArrayList(), openApiService.filterAndConvert(list, db, "table12"));
        Assert.assertEquals(Lists.newArrayList(), openApiService.filterAndConvert(list, db, "shard_"));

        // test check case-insensitive
        Assert.assertEquals(expectAll, openApiService.filterAndConvert(list, db, "TaBle1"));
        Assert.assertEquals(expectAll, openApiService.filterAndConvert(list, db.toUpperCase(), "TaBle1"));
        Assert.assertEquals(expectAll, openApiService.filterAndConvert(list, db.toUpperCase(), "table1"));
    }

    private static MhaDbReplicationDto getMhaDbReplicationDto(String srcRegion, String dstRegion, boolean drcStatus, List<String> logicTables) {
        MhaDbReplicationDto mhaDbReplicationDto = new MhaDbReplicationDto();
        mhaDbReplicationDto.setDrcStatus(drcStatus);
        List<DbReplicationDto> dbReplicationDtoList = logicTables.stream().map(e -> {
            LogicTableConfig config = new LogicTableConfig();
            config.setLogicTable(e);
            return new DbReplicationDto(-1L, config);
        }).toList();
        mhaDbReplicationDto.setDbReplicationDtos(dbReplicationDtoList);
        mhaDbReplicationDto.setSrc(getMhaDbDto(srcRegion));
        mhaDbReplicationDto.setDst(getMhaDbDto(dstRegion));
        return mhaDbReplicationDto;
    }

    private static MhaDbDto getMhaDbDto(String region) {
        DcDo dcDo = new DcDo();
        dcDo.setRegionName(region);
        MhaDbDto from = MhaDbDto.from(1L, new MhaTblV2(), new DbTbl(), dcDo);
        return from;
    }

}