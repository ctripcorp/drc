package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.schema.data.Columns;

import java.util.List;

/**
 * @Author Slight
 * Oct 24, 2019
 */
public class DecryptedDeleteRowsEvent extends MonitoredDeleteRowsEvent {

    public DecryptedDeleteRowsEvent(Columns columns) {
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
}
