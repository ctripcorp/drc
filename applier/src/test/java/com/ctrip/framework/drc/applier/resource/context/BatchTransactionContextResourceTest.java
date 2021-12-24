package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.applier.resource.mysql.DataSource;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @Author Slight
 * Apr 21, 2020
 */
public class BatchTransactionContextResourceTest extends TransactionContextResourceTest {


    BatchTransactionContextResource context;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void simple() throws Exception {
        context = spy(new BatchTransactionContextResource());
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.update(
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.000")),
                Bitmap.from(true, true, true),
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.001")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.insert(
                buildArray(buildArray(1, "Phi", "2019-12-09 16:00:02.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.insert(
                buildArray(buildArray(2, "Torch", "2019-12-09 16:00:03.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        assertEquals(TransactionData.ApplyResult.WHATEVER_ROLLBACK, context.complete());
        context.mustDispose();
        verify(context, times(0)).markDiscard();
    }

    @Test
    public void insertOnNewRow() throws Exception {
        context = new BatchTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(buildArray(1, "Phi", "2019-12-09 16:00:01.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.complete();
        context.dispose();

        context = spy(new BatchTransactionContextResource());
        context.dataSource = DataSource.wrap(dataSource);
        doNothing().when(context).setGtid(anyString());
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(buildArray(1, "Phy", "2019-12-09 16:00:00.001")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.insert(
                buildArray(buildArray(2, "Torch", "2019-12-09 17:00:00.001")),
                Bitmap.from(true, true, true),
                columns0()
        );
        assertEquals(TransactionData.ApplyResult.WHATEVER_ROLLBACK, context.complete());
        context.mustDispose();
        verify(context, times(0)).markDiscard();
    }

    @Test
    public void batchDeleteOnEmpty() throws Exception {
        context = spy(new BatchTransactionContextResource());
        context.dataSource = DataSource.wrap(dataSource);
        doNothing().when(context).setGtid(any());
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.delete(
                buildArray(buildArray(1, "2019-12-09 16:00:01.000")),
                Bitmap.from(true, false, true),
                columns0());
        context.delete(
                buildArray(buildArray(2, "2019-12-09 16:00:01.000")),
                Bitmap.from(true, false, true),
                columns0());
        context.delete(
                buildArray(buildArray(3, "2019-12-09 16:00:01.000")),
                Bitmap.from(true, false, true),
                columns0());
        assertEquals(TransactionData.ApplyResult.WHATEVER_ROLLBACK, context.complete());
        context.mustDispose();
        verify(context, times(0)).markDiscard();
    }

    @Test
    public void batchSuccess() throws Exception {
        context = new BatchTransactionContextResource();
        context.dataSource = DataSource.wrap(dataSource);
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(buildArray(1, "Phi", "2019-12-09 16:00:01.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.insert(
                buildArray(buildArray(2, "Phi", "2019-12-09 16:00:01.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.insert(
                buildArray(buildArray(3, "Phi", "2019-12-09 16:00:01.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.complete();
        context.dispose();

        context = spy(new BatchTransactionContextResource());
        context.dataSource = DataSource.wrap(dataSource);
        doNothing().when(context).setGtid(any());
        context.initialize();
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.insert(
                buildArray(buildArray(4, "Phi", "2019-12-09 16:00:01.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.update(
                buildArray(buildArray(4, "Torch", "2019-12-09 16:00:01.000")),
                Bitmap.from(true, true, true),
                buildArray(buildArray(4, "Torch", "2019-12-09 16:00:01.000")),
                Bitmap.from(true, true, true),
                columns0()
        );
        context.setTableKey(TableKey.from("prod", "hello1"));
        context.delete(
                buildArray(buildArray(1, "2019-12-09 16:00:01.000")),
                Bitmap.from(true, false, true),
                columns0());
        context.delete(
                buildArray(buildArray(2, "2019-12-09 16:00:01.000")),
                Bitmap.from(true, false, true),
                columns0());
        context.delete(
                buildArray(buildArray(3, "2019-12-09 16:00:01.000")),
                Bitmap.from(true, false, true),
                columns0());
        assertEquals(TransactionData.ApplyResult.SUCCESS, context.complete());
        context.mustDispose();
        verify(context, times(0)).markDiscard();
    }
}