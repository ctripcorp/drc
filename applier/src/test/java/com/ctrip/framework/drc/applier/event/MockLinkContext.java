package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.fetcher.resource.context.AbstractContext;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;
import com.ctrip.framework.drc.core.driver.schema.SchemasHistory;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @Author Slight
 * Oct 16, 2019
 */
public class MockLinkContext extends AbstractContext implements LinkContext {

    public Pair<TableKey, Columns> updatedTableKeyAndColumns;
    public String updatedGtid;
    public TableKey updatedTableKey;

    public Columns mockedColumns;
    public String mockedGtid;
    public TableKey mockedTableKey;
    public TableKey tableKeyUsedToFetchColumns;

    public MockLinkContext withMockedColumns(Columns columns) {
        mockedColumns = columns;
        return this;
    }

    public MockLinkContext withMockedGtid(String gtid) {
        mockedGtid = gtid;
        return this;
    }

    public MockLinkContext withMockedTableKey(TableKey key) {
        mockedTableKey = key;
        return this;
    }

    @Override
    public SchemasHistory getSchemasHistory() {
        //not used
        return null;
    }

    @Override
    public void updateSchema(TableKey tableKey, Columns columns) {
        updatedTableKeyAndColumns = new Pair(tableKey, columns);
    }

    @Override
    public Columns fetchColumns(TableKey tableKey) {
        tableKeyUsedToFetchColumns = tableKey;
        return mockedColumns;
    }

    @Override
    public void updateGtid(String gtid) {
        updatedGtid = gtid;
    }

    @Override
    public String fetchGtid() {
        return mockedGtid;
    }

    @Override
    public void updateTableKey(TableKey tableKey) {
        updatedTableKey = tableKey;
    }

    @Override
    public TableKey fetchTableKey() {
        return mockedTableKey;
    }

}
