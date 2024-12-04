package com.ctrip.framework.drc.applier.resource.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.ctrip.framework.drc.applier.activity.monitor.MetricsActivity;
import com.ctrip.framework.drc.applier.activity.monitor.entity.ConflictTable;
import com.ctrip.framework.drc.applier.confirmed.mysql.ConflictTest;
import com.ctrip.framework.drc.applier.event.ApplierColumnsRelatedTest;
import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.conflict.enums.ConflictResult;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author Slight
 * Sep 28, 2019
 */
public class ApplierTransactionContextResourceTest extends ConflictTest implements ApplierColumnsRelatedTest {

    ApplierTransactionContextResource context;

    protected  <T extends Object> ArrayList<T> buildArray(T... items) {
        return Lists.newArrayList(items);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void insertOnOldRow() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(buildArray(1, "Phi", "2019-12-09 15:00:01.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.commit();
        context.dispose();

        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.001")),
                Bitmap.from(true, true, true),
                columns0()
                );
        assertEquals(1, context.trxRecorder.getTrxRowNum());
        assertEquals(0, context.trxRecorder.getRollbackRowNum());
        assertEquals(1,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "hello1",
                ConflictResult.COMMIT.getValue()));
        assertEquals(1L,count.longValue());
        context.commit();
        context.dispose();
    }

    @Test
    public void insertOnNewRow() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(buildArray(1, "Phi", "2019-12-09 16:00:01.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.commit();
        context.dispose();

        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.001")),
                Bitmap.from(true, true, true),
                columns0()
                );
        assertEquals(1, context.trxRecorder.getTrxRowNum());
        assertEquals(1, context.trxRecorder.getRollbackRowNum());
        assertEquals(1,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "hello1",ConflictResult.ROLLBACK.getValue()));
        assertEquals(1L,count.longValue());
        context.commit();
        context.dispose();
    }

    @Test
    public void updateEmptyRow() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.update(
                buildArray(buildArray(1, "Mi", "2019-12-09 16:00:00.000")), Bitmap.from(true, true, true),
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.001")), Bitmap.from(true, true, true),
                columns0());
        assertEquals(1, context.trxRecorder.getTrxRowNum());
        assertEquals(0, context.trxRecorder.getRollbackRowNum());
        assertEquals(1,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "hello1",ConflictResult.COMMIT.getValue()));
        assertEquals(1L,count.longValue());
        context.dispose();
    }

    @Test
    public void updateOldRow() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(buildArray(1, "Phi", "2019-12-09 15:00:00.000")),
                Bitmap.from(true, true, true),
                columns0());

        context.dispose();

        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.update(
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.000")),
                Bitmap.from(true, true, true),
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.001")),
                Bitmap.from(true, true, true),
                columns0());
        assertEquals(1, context.trxRecorder.getTrxRowNum());
        assertEquals(0, context.trxRecorder.getRollbackRowNum());
        assertEquals(1,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "hello1",ConflictResult.COMMIT.getValue()));
        assertEquals(1L,count.longValue());
        context.dispose();
    }

    @Test
    public void updateNewRow() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(buildArray(1, "Phi", "2019-12-09 17:00:00.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.dispose();

        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.update(
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.000")),
                Bitmap.from(true, true, true),
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.001")),
                Bitmap.from(true, true, true),
                columns0());
        assertEquals(1, context.trxRecorder.getTrxRowNum());
        assertEquals(1, context.trxRecorder.getRollbackRowNum());
        assertEquals(1,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "hello1",ConflictResult.ROLLBACK.getValue()));
        assertEquals(1L,count.longValue());
        context.dispose();
    }

    @Test
    public void deleteEmptyRow() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.delete(
                buildArray(buildArray(1, "2019-12-09 16:00:00.001")),
                Bitmap.from(true, false, true),
                columns0()
        );
        assertEquals(1, context.trxRecorder.getTrxRowNum());
        assertEquals(0, context.trxRecorder.getRollbackRowNum());
        assertEquals(1,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "hello1",ConflictResult.COMMIT.getValue()));
        assertEquals(1L,count.longValue());
        context.dispose();
    }

    @Test
    public void twoUpdateWithOneHardConflict() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(
                        buildArray(1, "Phi", "2019-12-09 16:00:00.000"),
                        buildArray(2, "Sli", "2019-12-09 17:00:00.000")
                ),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.dispose();

        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.begin();
        context.update(
                buildArray(
                        buildArray(1, "Phy", "2019-12-09 16:00:00.000"),
                        buildArray(2, "Sly", "2019-12-09 16:00:00.000")
                ),
                Bitmap.from(true, true, true),
                buildArray(
                        buildArray(1, "Phy", "2019-12-09 16:00:00.001"),
                        buildArray(2, "Sly", "2019-12-09 16:00:00.001")
                ),
                Bitmap.from(true, true, true),
                columns0()
                );
        assertEquals(2, context.trxRecorder.getTrxRowNum());
        assertEquals(1, context.trxRecorder.getConflictRowNum());
        assertEquals(1, context.trxRecorder.getRollbackRowNum());
        assertEquals(1,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "hello1",ConflictResult.ROLLBACK.getValue()));
        assertEquals(1L,count.longValue());
        context.rollback();
        context.dispose();
    }

    @Test
    public void monitorBug() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.begin();
        context.insert(
                buildArray(
                        buildArray(3, "shaoy", "shaoy", "2019-12-23 19:18:17.281"),
                        buildArray(4, "sharb", "sharb", "2019-12-09 00:31:14.717")
                ),
                Bitmap.from(true, true, true, true),
                columns4());
        context.commit();
        context.dispose();

        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.begin();
        context.update(
                buildArray(buildArray(4, "sharb", "sharb", "2019-12-09 00:31:59.000")),
                Bitmap.from(true, true, true, true),
                buildArray(buildArray(4, "sharb", "sharb", "2019-12-09 00:32:00.000")),
                Bitmap.from(true, true, true, true),
                columns4()
        );
        assertEquals(1, context.trxRecorder.getTrxRowNum());
        assertEquals(1, context.trxRecorder.getConflictRowNum());
        assertEquals(0, context.trxRecorder.getRollbackRowNum());
        assertEquals(1,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "monitor",ConflictResult.COMMIT.getValue()));
        assertEquals(1L,count.longValue());
        context.commit();
    }

    @Test
    public void historyBreak() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.begin();
        context.insert(
                buildArray(
                        buildArray(3, "shaoy", "shaoy", "2019-12-23 19:18:17.281"),
                        buildArray(4, "sharb", "sharb", "2019-12-09 00:31:59.000")
                ),
                Bitmap.from(true, true, true, true),
                columns4()
        );
        context.commit();
        context.dispose();

        for (int i = 0; i < 2500; i++) {
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("insert into prod.hello1 (user) values ('p')");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.begin();
        context.update(
                buildArray(buildArray(4, "sharb", "sharb", "2019-12-09 00:31:59.000")),
                Bitmap.from(true, true, true, true),
                buildArray(buildArray(4, "sharb", "sharb", "2019-12-09 00:32:00.000")),
                Bitmap.from(true, true, true, true),
                columns4()
        );
        assertEquals(1, context.trxRecorder.getTrxRowNum());
        assertEquals(0, context.trxRecorder.getConflictRowNum());
        assertEquals(0, context.trxRecorder.getRollbackRowNum());
        assertEquals(0,context.trxRecorder.getConflictTableRowsCount().size());
        context.commit();
    }

    @Test
    public void deleteMultipleRows() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(
                        buildArray(1, "Phi", "2019-12-09 16:00:00.000"),
                        buildArray(2, "Sli", "2019-12-09 17:00:00.000")
                ),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.commit();
        context.dispose();

        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.begin();
        context.delete(
                buildArray(
                        buildArray(1, "2019-12-09 16:00:00.000"),
                        buildArray(2, "2019-12-09 17:00:00.000")
                ),
                Bitmap.from(true, false, true),
                columns0()
        );
        context.commit();
        context.dispose();

        assertEquals(0,context.trxRecorder.getConflictTableRowsCount().size());
        
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            assert statement.execute("select * from prod.hello1");
            assert !statement.getResultSet().last();
        }
        
    }

    @Test
    public void unknownColumnInsert2() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.begin();
        context.insert(
                buildArray(
                        buildArray(3, "shaoy", "shaoy", "none", "none2" ,"2019-12-23 19:18:17.281"),
                        buildArray(4, "sharb", "sharb", "none", "none2", "2019-12-09 00:31:59.000")
                ),
                Bitmap.from(true, true, true, true, true, true),
                columns5());
        context.commit();
        assertNull(context.getLastUnbearable());
        assertEquals(2, context.trxRecorder.getTrxRowNum());
        assertEquals(1, context.trxRecorder.getConflictRowNum());
        assertEquals(0, context.trxRecorder.getRollbackRowNum());
        assertEquals(1,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "monitor", ConflictResult.COMMIT.getValue()));
        assertEquals(1L,count.longValue());
        
        context.dispose();
    }

    @Test
    public void unknownColumnCurrentTimestamp() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.begin();
        context.insert(
                buildArray(
                        buildArray(5, "shaoy", "shaoy", "2019-12-23 19:18:17.281", "2019-12-23 19:18:17.281"),
                        buildArray(6, "sharb", "sharb", "2019-12-09 00:31:59.000", "2019-12-23 19:18:17.281")
                ),
                Bitmap.from(true, true, true, true, true),
                columns6());
        context.commit();
        assertNull(context.getLastUnbearable());
        assertEquals(2, context.trxRecorder.getTrxRowNum());
        assertEquals(1, context.trxRecorder.getConflictRowNum());
        assertEquals(0, context.trxRecorder.getRollbackRowNum());
        assertEquals(1,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "monitor", ConflictResult.COMMIT.getValue()));
        assertEquals(1L,count.longValue());
        context.dispose();
    }

    @Test
    public void unknownColumnUpdate2() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.begin();
        context.update(
                buildArray(
                        buildArray(4, "sharb", "sharb", "none", "none2", "2019-12-09 00:31:59.000"),
                        buildArray(5, "sharb", "sharb", "none", "none2", "2019-12-09 00:31:59.000")
                ),
                Bitmap.from(true, true, true, true, true, true),
                buildArray(
                        buildArray(4, "sharb", "sharb", "none", "none2", "2019-12-09 00:32:00.000"),
                        buildArray(5, "sharb", "sharb", "none", "none2", "2019-12-09 00:32:00.000")
                ),
                Bitmap.from(true, true, true, true, true, true),
                columns5()
        );
        context.commit();
        assertNull(context.getLastUnbearable());

        assertEquals(2, context.trxRecorder.getTrxRowNum());
        assertEquals(2, context.trxRecorder.getConflictRowNum());
        assertEquals(0, context.trxRecorder.getRollbackRowNum());
        assertEquals(1,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "monitor", ConflictResult.COMMIT.getValue()));
        assertEquals(2L,count.longValue());
        context.dispose();
    }

    @Test
    public void testCloneValues() {
        byte[] bytes = "Phi".getBytes();
        List<List<Object>> res = buildArray(buildArray(new BigDecimal(123), "Phi", null), buildArray(1, bytes, "2019-12-09 15:00:01.000"));
        context = new ApplierTransactionContextResource();
        List<List<Object>> clone = context.cloneListValues(res);
        List<Object> removed = clone.remove(0);
        Assert.assertNotEquals(clone.size(), res.size());
        Assert.assertEquals(clone.size(), 1);
        Assert.assertEquals(res.size(), 2);

        removed.remove(0);
        Assert.assertNotEquals(removed.size(), res.get(0).size());  // 2 - 3
        res.get(0).remove(0);
        Assert.assertArrayEquals(removed.toArray(), res.get(0).toArray());
    }

    @Test
    public void testEmptyTx() throws Exception {
        context = new ApplierTransactionContextResource();
        context.metricsActivity = new MetricsActivity();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(buildArray(1, "Phi", "2019-12-09 15:00:01.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.commit();
        context.dispose();

        context = new ApplierTransactionContextResource();
        context.metricsActivity = new MetricsActivity();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.commit();
        context.dispose();
    }

    @Test
    public void testLogMetricWhenUpdateDiffTable() throws Exception {
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        // init
        context.initialize();
        // table 1
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(
                        buildArray(1, "Phi", "2019-12-09 16:00:00.000"),
                        buildArray(2, "Sli", "2019-12-09 17:00:00.000")
                ),
                Bitmap.from(true, true, true),
                columns0()
        );
        // table 2
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.insert(
                buildArray(
                        buildArray(3, "shaoy", "shaoy", "2019-12-23 19:18:17.281"),
                        buildArray(4, "sharb", "sharb", "2019-12-09 00:31:14.717")
                ),
                Bitmap.from(true, true, true, true),
                columns4());
        context.dispose();


        // mock conflict
        context = new ApplierTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        // table 1
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.update(
                buildArray(
                        buildArray(1, "Phy", "2019-12-09 16:00:00.000"),
                        buildArray(2, "Sly", "2019-12-09 16:00:00.000")
                ),
                Bitmap.from(true, true, true),
                buildArray(
                        buildArray(1, "Phy", "2019-12-09 16:00:00.001"),
                        buildArray(2, "Sly", "2019-12-09 18:00:00.001")
                ),
                Bitmap.from(true, true, true),
                columns0()
        );
        // table 2
        context.setTableKey(TableKey.from("prod", "monitor"));
        context.update(
                buildArray(
                        buildArray(3, "shaoy", "shaoy", "2019-12-23 00:00:00.000"),
                        buildArray(4, "sharb", "sharb", "2019-12-09 00:00:00.000")
                ),
                Bitmap.from(true, true, true, true),
                buildArray(
                        buildArray(3, "shaoy", "shaoy", "2019-12-25 19:18:17.281"),
                        buildArray(4, "sharb", "sharb", "2019-12-19 00:31:14.717")
                ),
                Bitmap.from(true, true, true, true),
                columns4()
        );

        assertEquals(4, context.trxRecorder.getTrxRowNum());
        assertEquals(3, context.trxRecorder.getConflictRowNum());
        assertEquals(0, context.trxRecorder.getRollbackRowNum());
        assertEquals(2,context.trxRecorder.getConflictTableRowsCount().size());
        Long count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "hello1", ConflictResult.COMMIT.getValue()));
        assertEquals(1L,count.longValue());
        count = context.trxRecorder.getConflictTableRowsCount().get(new ConflictTable("prod", "monitor",  ConflictResult.COMMIT.getValue()));
        assertEquals(2L,count.longValue());
        context.commit();
        context.dispose();
    }
    

}
