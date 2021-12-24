package com.ctrip.framework.drc.console.monitor.consistency.cases;

import com.ctrip.framework.drc.console.monitor.consistency.sql.execution.NameAwareExecution;
import com.ctrip.framework.drc.console.monitor.consistency.sql.page.PagedExecution;
import com.ctrip.framework.drc.console.monitor.consistency.utils.RowCompareUtils;
import com.ctrip.framework.drc.core.monitor.entity.ConsistencyEntity;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.google.common.collect.Maps;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import static com.ctrip.framework.drc.console.monitor.consistency.sql.page.PagedExecution.LIMIT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_DC_LOGGER;

public class StreamQueryCheckPairCase extends RangeQueryCheckPairCase {

    private static final String DATA_CONSISTENCY_CHECK_MONITOR_COMPARE_NUM_MEASUREMENT = "fx.drc.dataCheck.compareNum";

    int srcTotalCount = 0;
    int dstTotalCount = 0;

    private ConsistencyEntity consistencyEntity;

    public StreamQueryCheckPairCase(NameAwareExecution execution) {
        super(execution);
    }

    public StreamQueryCheckPairCase(NameAwareExecution execution, ConsistencyEntity consistencyEntity) {
        super(execution);
        this.consistencyEntity = consistencyEntity;
    }

    private Reporter reporter = DefaultReporterHolder.getInstance();

    @Override
    public Map<String, Row> test(ReadSqlOperator<ReadResource> src, ReadSqlOperator<ReadResource> dst) {
        Map<String, Row> res = Maps.newHashMap();
        if (!(getExecution() instanceof PagedExecution)) {
            logger.error("execution must be instance of PageExecution");
            return res;
        }
        PagedExecution pagedExecution = (PagedExecution) getExecution();
        ReadResource srcResult = null;
        ReadResource dstResult = null;
        try {
            srcResult = src.select(getExecution());
            dstResult = dst.select(getExecution());
            do {
                res = RowCompareUtils.difference(res, toCompare(srcResult.getResultSet(), dstResult.getResultSet()));
                logger.info("[Compare] current diffRow size {}, src rows till now {}, dst rows till now {}, in {}", res.size(), srcTotalCount, dstTotalCount, this.getClass().getSimpleName());
            } while (pagedExecution.hasMore());

            if (srcTotalCount == 0 & dstTotalCount == 0) {
                reporter.reportResetCounter(consistencyEntity.getTags(), 1L, DATA_CONSISTENCY_CHECK_MONITOR_COMPARE_NUM_MEASUREMENT);
            } else {
                reporter.reportResetCounter(consistencyEntity.getTags(), (long) Math.max(srcTotalCount, dstTotalCount), DATA_CONSISTENCY_CHECK_MONITOR_COMPARE_NUM_MEASUREMENT);
            }
            logger.info("[Calculate] total diffRow size {}, total src rows {}, total dst rows {}, in {}", res.size(), srcTotalCount, dstTotalCount, this.getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("compare error", e);
        } finally {
            if (srcResult != null) {
                srcResult.close();
            }
            if (dstResult != null) {
                dstResult.close();
            }
            srcTotalCount = 0;
            dstTotalCount = 0;
        }
        return res;
    }

    @Override
    protected Map<String, Row> toCompare(ResultSet expected, ResultSet actual) throws SQLException {
        Map<String, Row> src = extract(expected);
        Map<String, Row> dst = extract(actual);
        int srcCount = src.size();
        int dstCount = dst.size();
        if (getExecution() instanceof PagedExecution) {
            PagedExecution pagedExecution = (PagedExecution) getExecution();
            pagedExecution.setResultSize(Math.max(srcCount, dstCount));
        }

        logger.info("[Compare] src rows {}, dst rows {}", srcCount, dstCount);
        srcTotalCount += srcCount;
        dstTotalCount += dstCount;
        return RowCompareUtils.difference(src, dst);
    }

    /**
     * Since it is stream, instead of read all resultSet, stream read maximum LIMIT lines
     */
    @Override
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

        int lineCount = 1;
        while (lineCount % (LIMIT + 1) != 0 && resultSet.next()) {
            Row row = new Row();
            String key = null;
            for (int i = 1; i <= srcColumnCount; ++i) {
                String columnName = src.getColumnName(i);
                Object value = resultSet.getObject(i);
                if (columnName.equalsIgnoreCase(getExecution().getKey())) {
                    key = String.valueOf(value);
                }
                row.addField(value);
            }
            queryResult.put(key, row);
            lineCount++;
        }
        CONSOLE_DC_LOGGER.debug("[{}] queryResult size {}", Thread.currentThread().getName(), queryResult.size());

        return queryResult;
    }
}
