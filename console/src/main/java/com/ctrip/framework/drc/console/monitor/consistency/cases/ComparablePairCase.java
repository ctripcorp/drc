package com.ctrip.framework.drc.console.monitor.consistency.cases;

import com.ctrip.framework.drc.console.monitor.consistency.sql.execution.NameAwareExecution;
import com.ctrip.framework.drc.console.monitor.consistency.sql.page.PagedExecution;
import com.ctrip.framework.drc.console.monitor.consistency.utils.RowCompareUtils;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by mingdongli
 * 2019/11/15 上午9:56.
 */
public class ComparablePairCase implements CallablePairCase<Map<String, Row>> {

    protected Logger logger = LoggerFactory.getLogger("consistencyMonitorLogger");

    private NameAwareExecution execution;

    public NameAwareExecution getExecution() {
        return execution;
    }

    public ComparablePairCase(NameAwareExecution execution) {
        this.execution = execution;
    }

    /**
     * @param src QuerySqlOperatorWrapper
     * @param dst QuerySqlOperatorWrapper
     */
    @Override
    public Map<String, Row> test(ReadSqlOperator<ReadResource> src, ReadSqlOperator<ReadResource> dst) {
        ReadResource srcResult = null;
        ReadResource dstResult = null;
        try {
            srcResult = src.select(execution);
            dstResult = dst.select(execution);
            return toCompare(srcResult.getResultSet(), dstResult.getResultSet());
        } catch (Exception e) {
            logger.error("compare error", e);
        } finally {
            if (srcResult != null) {
                srcResult.close();
            }
            if (dstResult != null) {
                dstResult.close();
            }
        }
        return Maps.newHashMap();
    }

    protected Map<String, Row> toCompare(ResultSet expected, ResultSet actual) throws SQLException {
        Map<String, Row> src = extract(expected);
        Map<String, Row> dst = extract(actual);
        if (execution instanceof PagedExecution) {
            PagedExecution pagedExecution = (PagedExecution) execution;
            pagedExecution.setResultSize(Math.max(src.size(), dst.size()));
        }

        logger.info("[Compare] src rows {}, dst rows {}", src.size(), dst.size());
        return RowCompareUtils.difference(src, dst);
    }

    protected Map<String, Row> extract(ResultSet resultSet) throws SQLException {  // current row not null

        Map<String, Row> queryResult = Maps.newLinkedHashMap();
        if (resultSet == null) {
            return queryResult;
        }
        ResultSetMetaData src = resultSet.getMetaData();
        if (src == null) {
            return queryResult;
        }
        int srcColumnCount = src.getColumnCount();

        while (resultSet.next()) {
            Row row = new Row();
            String key = null;
            for (int i = 1; i <= srcColumnCount; ++i) {
                String columnName = src.getColumnName(i);
                Object value = resultSet.getObject(i);
                if (columnName.equalsIgnoreCase(execution.getKey())) {
                    key = String.valueOf(value);
                }
                row.addField(value);
            }
            queryResult.put(key, row);
        }

        return queryResult;
    }

}