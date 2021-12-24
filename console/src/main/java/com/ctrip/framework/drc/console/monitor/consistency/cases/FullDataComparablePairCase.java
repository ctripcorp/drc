package com.ctrip.framework.drc.console.monitor.consistency.cases;

import com.ctrip.framework.drc.console.monitor.consistency.sql.execution.NameAwareExecution;
import com.ctrip.framework.drc.console.monitor.consistency.utils.RowCompareUtils;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jixinwang on 2021/2/20
 */
public class FullDataComparablePairCase extends RangeQueryCheckPairCase {
    private static final int BATCH_SIZE = 1000;

    private static final int DIFF_COUNT_LIMIT = MonitorTableSourceProvider.getInstance().getDiffCountLimit();

    private int srcDispatchTotalCount = 0;

    private int dstDispatchTotalCount = 0;

    private AtomicInteger srcCompareTotalCount = new AtomicInteger(0);

    private AtomicInteger dstCompareTotalCount = new AtomicInteger(0);

    private volatile boolean dispatchFlag = true;

    private AtomicInteger dispatchBatchCount = new AtomicInteger(0);

    private AtomicInteger compareBatchCount = new AtomicInteger(0);

    Map<String, Row> lastTimeSrcQueryResult = Maps.newConcurrentMap();

    Map<String, Row> lastTimeDstQueryResult = Maps.newConcurrentMap();

    Map<String, Row> curSrcQueryResult = Maps.newConcurrentMap();

    Map<String, Row> curDstQueryResult = Maps.newConcurrentMap();

    Map<String, Row> nextSrcQueryResult = Maps.newConcurrentMap();

    Map<String, Row> nextDstQueryResult = Maps.newConcurrentMap();

    private Map<String, Row> diffResult = Maps.newConcurrentMap();

    private LinkedBlockingQueue<QueryResultGroup> queryResultGroupQueue = new LinkedBlockingQueue<>(5);

    private ExecutorService dispatchExecutor = ThreadUtils.newFixedThreadPool(1, "dispatchExecutor");

//    private ExecutorService startCompareExecutor = ThreadUtils.newFixedThreadPool(1, "startCompareExecutor");

    private ExecutorService compareExecutor = ThreadUtils.newFixedThreadPool(2, "compareExecutor");

    public FullDataComparablePairCase(NameAwareExecution execution) {
        super(execution);
    }


    @Override
    public Map<String, Row> test(ReadSqlOperator<ReadResource> src, ReadSqlOperator<ReadResource> dst) {
        long start = System.currentTimeMillis();

        //start dispatch
        dispatchExecutor.submit(new Runnable() {
            @Override
            public void run() {
                startDispatch(src, dst);
            }
        });

        //start compare
        while (dispatchFlag || !queryResultGroupQueue.isEmpty()) {
            if (diffResult.size() >= DIFF_COUNT_LIMIT) {
                dispatchFlag = false;
                logger.error("[FULL DATA CHECK] different count is over 100, different count is {}", compareBatchCount);
                break;
            }
            startCompare();
        }

        dispatchExecutor.shutdown();
        compareExecutor.shutdown();
        long end = System.currentTimeMillis();
        logger.info("[FULL DATA CHECK] compare record count is: {}, consume time is: {} seconds", srcCompareTotalCount, (end - start) / 1000);
        return diffResult;
    }

    private void startDispatch(ReadSqlOperator<ReadResource> src, ReadSqlOperator<ReadResource> dst) {
        try (ReadResource srcResult = src.select(getExecution()); ReadResource dstResult = dst.select(getExecution())) {
            if (null == srcResult || null == dstResult) {
                logger.error("[FULL DATA CHECK] start dispatch error,select result is null");
                return;
            }
            dispatch(srcResult.getResultSet(), dstResult.getResultSet());
        } catch (SQLException e) {
            logger.error("[FULL DATA CHECK] start dispatch error", e);
        }
    }

    private void dispatch(ResultSet src, ResultSet dst) {
        while (dispatchFlag) {
            try {
                Map<String, Row> srcQueryResult = extract(src);
                Map<String, Row> dstQueryResult = extract(dst);

                getTimestampBoundary(src, dst);
                srcQueryResult.putAll(lastTimeSrcQueryResult);
                dstQueryResult.putAll(lastTimeDstQueryResult);
                srcQueryResult.putAll(curSrcQueryResult);
                dstQueryResult.putAll(curDstQueryResult);
                lastTimeSrcQueryResult = nextSrcQueryResult;
                lastTimeDstQueryResult = nextDstQueryResult;
                if (srcQueryResult.isEmpty() && dstQueryResult.isEmpty()) {
                    dispatchFlag = false;
                    logger.info("[FULL DATA CHECK] end dispatch batch");
                    break;
                } else {
                    logger.info("[FULL DATA CHECK] dispatch batch srcCount is {}, dstCount is {}", srcQueryResult.size(), dstQueryResult.size());
                    srcDispatchTotalCount += srcQueryResult.size();
                    dstDispatchTotalCount += dstQueryResult.size();
                    logger.info("[FULL DATA CHECK] dispatch all srcCount is {}, dstCount is {}", srcDispatchTotalCount, dstDispatchTotalCount);
                    QueryResultGroup compareGroup = new QueryResultGroup(srcQueryResult, dstQueryResult);
                    queryResultGroupQueue.put(compareGroup);
                    dispatchBatchCount.incrementAndGet();
                    logger.info("[FULL DATA CHECK] dispatch batch count is {}", dispatchBatchCount);
                }
            } catch (SQLException | InterruptedException e) {
                logger.error("[FULL DATA CHECK] dispatch error", e);
                break;
            }

        }
    }

    public void compareQueryResultGroup(QueryResultGroup queryResultGroup) {

        srcCompareTotalCount.addAndGet(queryResultGroup.getSrcQueryResult().size());
        dstCompareTotalCount.addAndGet(queryResultGroup.getDstQueryResult().size());
        diffResult.putAll(RowCompareUtils.difference(queryResultGroup.getSrcQueryResult(), queryResultGroup.getDstQueryResult()));

        logger.info("[FULL DATA CHECK] compare all srcCount is {}, dstCount is {}", srcCompareTotalCount, dstCompareTotalCount);
        compareBatchCount.incrementAndGet();
        logger.info("[FULL DATA CHECK] compare batch count is {}", compareBatchCount);
        if (diffResult.size() >= DIFF_COUNT_LIMIT) {
            dispatchFlag = false;
        }
        logger.info("[FULL DATA CHECK] batch count is {}, different count is: {}", compareBatchCount, diffResult.size());
    }

    public void startCompare() {
        try {
            QueryResultGroup queryResultGroup = queryResultGroupQueue.poll(10, TimeUnit.SECONDS);
            if (queryResultGroup == null) {
                return;
            }
            compareExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    compareQueryResultGroup(queryResultGroup);
                }
            });
        } catch (InterruptedException e) {
            logger.error("[FULL DATA CHECK] compare batch error {}", e.getMessage());
        }

    }

    //extract batch result
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

        long curCount = 0;
        while (resultSet.next()) {
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
            curCount++;
            if (curCount % (BATCH_SIZE) == 0) {
                return queryResult;
            }
        }
        return queryResult;
    }

    // with timestamp one row
    protected BaseQueryResult extractSingleQueryResult(ResultSet resultSet) throws SQLException {  // current row not null

        BaseQueryResult baseQueryResult = new BaseQueryResult();
        Map<String, Row> queryResult = Maps.newLinkedHashMap();
        if (resultSet == null) {
            return baseQueryResult;
        }
//        logger.info("[FULL DATA CHECK] ahead resultFetchSize is {},result is {}", resultSet.getFetchSize(), resultSet);
        ResultSetMetaData src = resultSet.getMetaData();
        if (src == null) {
            return baseQueryResult;
        }
        int srcColumnCount = src.getColumnCount();

        if (!resultSet.next()) {
            return baseQueryResult;
        }
        Row row = new Row();
        String key = null;
        Timestamp onUpdate = null;
//        logger.info("[FULL DATA CHECK] after resultFetchSize is {},result is {}", resultSet.getFetchSize(), resultSet);
        for (int i = 1; i <= srcColumnCount; ++i) {
            String columnName = src.getColumnName(i);
            Object value = resultSet.getObject(i);
            if (columnName.equalsIgnoreCase(getExecution().getKey())) {
                key = String.valueOf(value);
            }
            if (columnName.equalsIgnoreCase(getExecution().getOnUpdate())) {
                onUpdate = Timestamp.valueOf(String.valueOf(value));
            }
            row.addField(value);
        }
        queryResult.put(key, row);
        baseQueryResult.setQueryResult(queryResult);
        baseQueryResult.setOnUpdate(onUpdate);
        return baseQueryResult;
    }

    public void getTimestampBoundary(ResultSet src, ResultSet dst) throws SQLException {
        BaseQueryResult srcSingleQueryResult = extractSingleQueryResult(src);
        BaseQueryResult dstSingleQueryResult = extractSingleQueryResult(dst);
        nextSrcQueryResult.clear();
        nextDstQueryResult.clear();
        curSrcQueryResult.clear();
        curDstQueryResult.clear();

        Timestamp srcTimeStamp = srcSingleQueryResult.getOnUpdate();
        Timestamp dstTimeStamp = dstSingleQueryResult.getOnUpdate();
        if (srcTimeStamp == null && dstTimeStamp == null) {
            return;
        }
        if (srcTimeStamp == null) {// means src is already select all
            // next row's timestamp
            curDstQueryResult = dstSingleQueryResult.getQueryResult();
            long expireTimestamp = dstTimeStamp.getTime();
            searchToExpireTimestamp(expireTimestamp, dst, curDstQueryResult, nextDstQueryResult);
            return;

        }
        if (dstTimeStamp == null) {
            curSrcQueryResult = srcSingleQueryResult.getQueryResult();
            long expireTimestamp = srcTimeStamp.getTime();
            searchToExpireTimestamp(expireTimestamp, src, curSrcQueryResult, nextSrcQueryResult);
            return;
        }

        curSrcQueryResult = srcSingleQueryResult.getQueryResult();
        curDstQueryResult = dstSingleQueryResult.getQueryResult();
        long expireTimestamp = srcTimeStamp.getTime();
        searchToExpireTimestamp(expireTimestamp, src, curSrcQueryResult, nextSrcQueryResult);
        searchToExpireTimestamp(expireTimestamp, dst, curDstQueryResult, nextDstQueryResult);
    }

    public void searchToExpireTimestamp(long expireTimestamp, ResultSet rs, Map<String, Row> curQueryResult, Map<String, Row> nextQueryResult) throws SQLException {
        while (true) {
            // search for all timestamp later than expireTimeStamp and put it in curQueryResult
            BaseQueryResult queryResult = extractSingleQueryResult(rs);
            Timestamp SingleQueryTimestamp = queryResult.getOnUpdate();
            if (SingleQueryTimestamp == null) {
                break;
            }
            // later
            if (SingleQueryTimestamp.getTime() >= expireTimestamp) {
                curQueryResult.putAll(queryResult.getQueryResult());
            } else {
                //earlier than expire ,put in nextQuery
                nextQueryResult.putAll(queryResult.getQueryResult());
                break;
            }
        }
    }
}
