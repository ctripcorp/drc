package com.ctrip.framework.drc.validation.resource.context;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.monitor.enums.DmlEnum;
import com.ctrip.framework.drc.validation.AllTests;
import com.ctrip.framework.drc.validation.activity.event.ValidationColumnsRelatedTest;
import com.ctrip.framework.ucs.client.api.RequestContext;
import com.ctrip.framework.ucs.client.api.UcsClient;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.ctrip.framework.drc.validation.AllTests.*;

public class ValidationTransactionContextResourceTest implements ValidationColumnsRelatedTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private ValidationTransactionContextResource context = new ValidationTransactionContextResource();

    protected  <T extends Object> ArrayList<T> buildArray(T... items) {
        return com.google.common.collect.Lists.newArrayList(items);
    }

    public static final long EXECUTE_TIME = 1L;

    @Before
    public void setUp() throws Exception {
        context = new ValidationTransactionContextResource();
        context.initialize();
        context.setTableKey(new TableKey(DATABASE, TABLE));
        context.setExecuteTime(EXECUTE_TIME);
        context.updateGtid(GTID);
        context.mhaName = MHA;
        context.uidMap = UID_MAP;
        context.ucsStrategyIdMap = UCS_STRATEGY_MAP;
        context.machines = AllTests.getMachines();

        UcsClient ucsClient = Mockito.mock(UcsClient.class);
        RequestContext requestContext = Mockito.mock(RequestContext.class);
        context.setUcsClient(ucsClient);
        Mockito.when(ucsClient.buildRequestContext(Mockito.anyString(), Mockito.anyInt())).thenReturn(requestContext);
        Mockito.when(requestContext.getRequestZone()).thenReturn(Optional.of(DC));
    }

    @After
    public void tearDown() throws Exception {
        context.dispose();
    }

    @Test
    public void testSetLastUnbearable() {
        context.setLastUnbearable(new Throwable());
    }

    @Test
    public void testSetTableKey() {
        context.setTableKey(new TableKey(DATABASE, TABLE));
        TableKey tableKey = context.fetchTableKey();
        Assert.assertEquals(DATABASE, tableKey.getDatabaseName());
        Assert.assertEquals(TABLE, tableKey.getTableName());
    }

    @Test
    public void testSetExecuteTime() {
        long executeTime = 1L;
        context.setExecuteTime(executeTime);
        Assert.assertEquals(executeTime, context.fetchExecuteTime());
    }

    @Test
    public void testInsert() {
        context.insert(
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.001")),
                Bitmap.from(true, true, true),
                columns0()
        );
    }

    @Test
    public void testUpdate() {
        context.update(
                buildArray(buildArray(1, "Mi", "2019-12-09 16:00:00.000")), Bitmap.from(true, true, true),
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.001")), Bitmap.from(true, true, true),
                columns0());
    }

    @Test
    public void testDelete() {
        context.delete(
                buildArray(buildArray(1, "2019-12-09 16:00:00.001")),
                Bitmap.from(true, false, true),
                columns0()
        );
    }

    @Test
    public void testValidation1() {
        context.setTableKey(new TableKey("drcmonitordb", "delaymonitor"));
        validationAny();
        Assert.assertEquals(0L, context.incorrectResCnt);
        Assert.assertEquals(0L, context.correctResCnt);
        Assert.assertEquals(0L, context.totalResCnt);
        Assert.assertEquals(1L, context.totalParseCnt);
    }

    @Test
    public void testValidation2() {
        context.setTableKey(new TableKey("nosuchdb", "nosuchtable"));
        validationAny();
        Assert.assertEquals(0L, context.incorrectResCnt);
        Assert.assertEquals(0L, context.correctResCnt);
        Assert.assertEquals(0L, context.totalResCnt);
        Assert.assertEquals(1L, context.totalParseCnt);
    }

    @Test
    public void testValidation3() {
        context.uidMap = UID_MAP_INCORRECT;
        context.ucsStrategyIdMap = UCS_STRATEGY_MAP_INCORRECT;
        validationInsert();
        Assert.assertEquals(0L, context.incorrectResCnt);
        Assert.assertEquals(0L, context.correctResCnt);
        Assert.assertEquals(0L, context.totalResCnt);
        Assert.assertEquals(1L, context.totalParseCnt);
    }

    @Test
    public void testValidation4() {
        context.machines = getMachines2();
        validationInsert();
        Assert.assertEquals(0L, context.incorrectResCnt);
        Assert.assertEquals(0L, context.correctResCnt);
        Assert.assertEquals(1L, context.totalResCnt);
        Assert.assertEquals(1L, context.totalParseCnt);
    }

    @Test
    public void testValidation5() {
        context.machines = getMachines3();
        validationInsert();
        logger.info("incorrect:{}, correct:{}, totalRes:{}, totalParse:{}, resultDtos:{}", context.incorrectResCnt, context.correctResCnt, context.totalResCnt, context.totalParseCnt, context.resultDtos.size());
        Assert.assertEquals(1L, context.incorrectResCnt);
        Assert.assertEquals(0L, context.correctResCnt);
        Assert.assertEquals(1L, context.totalResCnt);
        Assert.assertEquals(1L, context.totalParseCnt);
        Assert.assertEquals(1, context.resultDtos.size());
    }

    @Test
    public void testValidation6() {
        validationInsert();
        logger.info("incorrect:{}, correct:{}, totalRes:{}, totalParse:{}, resultDtos:{}", context.incorrectResCnt, context.correctResCnt, context.totalResCnt, context.totalParseCnt, context.resultDtos.size());
        Assert.assertEquals(0L, context.incorrectResCnt);
        Assert.assertEquals(1L, context.correctResCnt);
        Assert.assertEquals(1L, context.totalResCnt);
        Assert.assertEquals(1L, context.totalParseCnt);
    }

    private void validationAny() {
        context.validation(null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private void validationInsert() {
        context.validation(buildArray(1, "Phil Colson", "male", UID_VALUE, "2021-03-28 23:00:00.001"),
                Bitmap.from(true, true, true, true, true),
                null,
                null,
                columns6(),
                "insert into `db1`.`tbl1`(`id`, `user`, `gender`, `uid1`, `lt`) values(1, \"Phil Colson\", \"male\", \"M0001\", \"2021-03-28 23:00:00.001\");",
                DmlEnum.INSERT
        );
    }

    private void validationUpdate() {
        context.validation(buildArray(1, "Phil Colson", "male", UID_VALUE, "2021-03-28 23:00:00.001"),
                Bitmap.from(true, true, true, true, true),
                buildArray(buildArray(1, "Phil Coulson", "male", UID_VALUE, "2021-03-28 23:00:00.001")),
                Bitmap.from(true, true, true, true, true),
                columns6(),
                "update `db1`.`tbl1` set `user` = \"Phil Coulson\" where `id` = 1;",
                DmlEnum.UPDATE
        );
    }

    private void validationDelete() {
        context.validation(buildArray(1, "Phil Coulson", "male", UID_VALUE, "2021-03-28 23:00:00.001"),
                Bitmap.from(true, true, true, true, true),
                null,
                null,
                columns6(),
                "delete from `db1`.`tbl1` where `id` = 1;",
                DmlEnum.DELETE
        );
    }

    @Test
    public void testConvertObjectToString() {
        List<Object> objectList = buildArray(1, "Phil Coulson", "male", UID_VALUE, "2021-03-28 23:00:00.001");
        List<String> actual = context.convertObjectToString(objectList);
        List<String> expected = Arrays.asList("1", "Phil Coulson", "male", UID_VALUE, "2021-03-28 23:00:00.001");
        Assert.assertEquals(expected, actual);

        objectList = buildArray(1, "Phil Coulson", null, UID_VALUE, "2021-03-28 23:00:00.001");
        actual = context.convertObjectToString(objectList);
        expected = Arrays.asList("1", "Phil Coulson", null, UID_VALUE, "2021-03-28 23:00:00.001");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testIndexOf() {
        List<String> columns = Lists.newArrayList("id", "UID", "datachange_last_time");
        Assert.assertTrue(columns.indexOf("uid") == -1);
        Assert.assertEquals(1, ValidationTransactionContextResource.caseInsensitiveIndexOf(columns, "uid"));
    }

}
