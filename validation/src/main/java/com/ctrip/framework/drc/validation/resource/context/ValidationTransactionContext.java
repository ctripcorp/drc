package com.ctrip.framework.drc.validation.resource.context;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.monitor.enums.DmlEnum;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;

import java.util.List;

/**
 * @Author Slight
 * Sep 26, 2019
 */
public interface ValidationTransactionContext extends BaseTransactionContext {

    void validation(List<Object> beforeRow, Bitmap beforeBitmap, List<Object> afterRow, Bitmap afterBitmap, Columns columns, String sql, DmlEnum dmlEnum);

    void setExecuteTime(long executeTime);
}
