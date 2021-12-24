package com.ctrip.framework.drc.console.monitor.consistency.sql.execution;

import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by mingdongli
 * 2019/11/15 下午4:05.
 */
public abstract class AbstractNameAwareExecution implements NameAwareExecution {

    protected Logger logger = LoggerFactory.getLogger("consistencyMonitorLogger");

    private TableNameDescriptor tableDescriptor;

    public AbstractNameAwareExecution(TableNameDescriptor tableDescriptor) {
        this.tableDescriptor = tableDescriptor;
    }

    @Override
    public String getStatement() {
        String sql = getQuerySql();
        logger.info("[Statement] is {}", sql);
        return sql;
    }

    protected abstract String getQuerySql();

    @Override
    public Collection<String> getStatements() {
        return null;
    }

    @Override
    public boolean appendStatements(Collection<String> statement) {
        return false;
    }

    @Override
    public boolean replaceStatements(Collection<String> statement) {
        return false;
    }

    @Override
    public String getKey() {
        return tableDescriptor.getKey();
    }

    @Override
    public String getTable() {
        return tableDescriptor.getTable();
    }

    @Override
    public String getOnUpdate() {
        return tableDescriptor.getOnUpdate();
    }
}
