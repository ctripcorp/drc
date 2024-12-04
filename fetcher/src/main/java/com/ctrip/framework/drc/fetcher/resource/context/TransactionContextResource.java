package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;
import com.ctrip.framework.drc.fetcher.resource.condition.Progress;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.system.*;
import org.apache.commons.lang3.StringUtils;


import java.util.Arrays;

import static com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType.isDatetimePrecisionType;

/**
 * @Author Slight
 * Sep 27, 2019
 */
public abstract class TransactionContextResource extends AbstractContext
        implements TransactionContext, Resource.Dynamic {

    @InstanceResource
    public Progress progress;

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    //state of one transaction
    protected TableKey tableKey;
    protected boolean transactionTableConflict;
    public Throwable lastUnbearable;

    public static int RECORD_SIZE = 100;

    public boolean isTransactionTableConflict() {
        return transactionTableConflict;
    }

    public void setTransactionTableConflict(boolean transactionTableConflict) {
        this.transactionTableConflict = transactionTableConflict;
    }

    @Override
    public void setTableKey(TableKey tableKey) {
        this.tableKey = tableKey;
        updateTableKey(tableKey);
    }

    @Override
    public void setLastUnbearable(Throwable throwable) {
        this.lastUnbearable = throwable;
    }

    public static void assertDefault(Object value, Object columnDefault, int type) {
        if (value == null) {
            assert columnDefault == null
                    : "columnDefault != null";
        } else {
            if (value instanceof byte[] && columnDefault instanceof byte[]) {
                assert Arrays.equals((byte[])value, (byte[])columnDefault)
                        : "value not equals columnDefault as byte[]";
            } else {
                if (isDatetimePrecisionType(type) && StringUtils.containsIgnoreCase((String) columnDefault, "CURRENT_TIMESTAMP")) {
                    return;
                }
                assert value.equals(columnDefault)
                        : "value not equals columnDefault";
            }
        }
    }


    public String toString() {
        try {
            return "(jdbc)" + fetchGtid();
        } catch (Throwable t) {
            return "(jdbc)unset";
        }
    }


    @Override
    public void setPhaseName(String name) {
        previousPhaseName.set(phaseName.get());
        phaseName.set(name);
    }


}
