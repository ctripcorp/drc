package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationResultDto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class UnitServiceImplTest {

    @InjectMocks
    private UnitServiceImpl unitService;

    @Mock
    private MetaInfoServiceImpl metaInfoService;

    public static final String SCHEMA = "testdb";
    public static final String TABLE = "testtable";
    public static final String MHA = "testmha";
    public static final String GTID = "uuid0:1-10";
    public static final String SQL = "select 1";
    public static final String EXPECTED_DC = "shaoy";
    public static final String ACTUAL_DC = "sharb";
    public static final List<String> COLUMNS = Arrays.asList("id", "name");
    public static final List<String> BEFORE_VALUES = Arrays.asList("1", "name1");
    public static final List<String> AFTER_VALUES = Arrays.asList("1", "name2");
    public static final String UID_NAME = "uid";
    public static final int UCS_STRATEGY_ID = 1;
    public static final long EXECUTE_TIME = 100L;

    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        Mockito.doReturn(1L).when(metaInfoService).getMhaGroupId(MHA);
    }

    @Test
    public void testAddUnitRouteVerificationResult() throws SQLException {
        ValidationResultDto dto = new ValidationResultDto();
        dto.setSchemaName(SCHEMA);
        dto.setTableName(TABLE);
        dto.setGtid(GTID);
        dto.setSql(SQL);
        dto.setExpectedDc(EXPECTED_DC);
        dto.setActualDc(ACTUAL_DC);
        dto.setColumns(COLUMNS);
        dto.setBeforeValues(BEFORE_VALUES);
        dto.setAfterValues(AFTER_VALUES);
        dto.setUidName(UID_NAME);
        dto.setUcsStrategyId(UCS_STRATEGY_ID);
        dto.setMhaName(MHA);
        dto.setExecuteTime(EXECUTE_TIME);
        Assert.assertTrue(unitService.addUnitRouteVerificationResult(dto));
    }

    @Test
    public void testGetUnitRouteVerificationResult() throws SQLException {
        Assert.assertEquals(1, unitService.getUnitRouteVerificationResult(MHA, SCHEMA, TABLE).size());
        Assert.assertEquals(1, unitService.getUnitRouteVerificationResult(MHA, "", "").size());

        Mockito.doThrow(new SQLException()).when(metaInfoService).getMhaGroupId(MHA);
        Assert.assertEquals(0, unitService.getUnitRouteVerificationResult(MHA, SCHEMA, TABLE).size());
    }
}
