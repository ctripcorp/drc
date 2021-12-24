package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import com.ctrip.framework.drc.fetcher.system.AbstractSystem;
import com.ctrip.framework.drc.fetcher.system.Unit;

import java.util.List;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class MockTransactionContext implements BaseTransactionContext {

    @Override
    public void setLastUnbearable(Throwable throwable) {

    }

    @Override
    public void setTableKey(TableKey tableKey) {

    }

    @Override
    public void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {

    }

    @Override
    public void update(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns) {

    }

    @Override
    public void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {

    }

    @Override
    public void update(String key, Object value) {

    }

    @Override
    public Object fetch(String key) {
        return null;
    }

    @Override
    public void setSystem(AbstractSystem system) {

    }

    @Override
    public AbstractSystem getSystem() {
        return null;
    }

    @Override
    public void load() throws Exception {

    }

    @Override
    public Unit derive(Class clazz) throws Exception {
        return null;
    }

    @Override
    public String namespace() {
        return null;
    }

    @Override
    public void dispose() throws Exception {

    }

    @Override
    public void initialize() throws Exception {

    }
}
