package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.schema.data.Columns;

import java.util.List;

/**
 * @Author Slight
 * Oct 23, 2019
 */
public class DecryptedUpdateRowsEvent extends MonitoredUpdateRowsEvent {

    public DecryptedUpdateRowsEvent(Columns columns) {
        this.columns = columns;
    }

    @Override
    public List<Boolean> getBeforeRowsKeysPresent() {
        return null;
    }

    @Override
    public List<List<Object>> getBeforePresentRowsValues() {
        return null;
    }

    @Override
    public List<List<Object>> getAfterPresentRowsValues() {
        return null;
    }

    @Override
    public List<Boolean> getAfterRowsKeysPresent() {
        return null;
    }
}
