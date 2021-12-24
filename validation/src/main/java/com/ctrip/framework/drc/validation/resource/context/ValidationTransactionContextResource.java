package com.ctrip.framework.drc.validation.resource.context;

import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.monitor.enums.DmlEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationResultDto;
import com.ctrip.framework.drc.fetcher.resource.context.AbstractContext;
import com.ctrip.framework.drc.fetcher.resource.context.sql.DeleteBuilder;
import com.ctrip.framework.drc.fetcher.resource.context.sql.InsertBuilder;
import com.ctrip.framework.drc.fetcher.resource.context.sql.UpdateBuilder;
import com.ctrip.framework.drc.fetcher.system.InstanceActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.validation.activity.monitor.ValidationMetricsActivity;
import com.ctrip.framework.drc.validation.activity.monitor.ValidationResultActivity;
import com.ctrip.framework.ucs.client.api.RequestContext;
import com.ctrip.framework.ucs.client.api.UcsClient;
import com.ctrip.framework.ucs.client.api.UcsClientFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/17
 */
public class ValidationTransactionContextResource extends AbstractContext implements ValidationTransactionContext {

    @InstanceConfig(path="mhaName")
    public String mhaName;

    @InstanceConfig(path="uidMap")
    public Map<String, String> uidMap;

    @InstanceConfig(path="ucsStrategyIdMap")
    public Map<String, Integer> ucsStrategyIdMap;

    @InstanceConfig(path="machines")
    public List<DBInfo> machines;

    @InstanceActivity
    public ValidationMetricsActivity metricsActivity;

    @InstanceActivity
    public ValidationResultActivity resultActivity;

    private UcsClient ucsClient = UcsClientFactory.getInstance().getUcsClient();

    protected List<ValidationResultDto> resultDtos;
    protected long incorrectResCnt = 0;
    protected long correctResCnt = 0;
    protected long totalResCnt = 0;
    protected long totalParseCnt = 0;

    private static final String UNKNOWN_ZONE = "unknown";

    public static final String EXCLUDE_DB = "`drcmonitordb`.`delaymonitor`";

    @Override
    public void setLastUnbearable(Throwable throwable) {

    }

    @Override
    public void setTableKey(TableKey tableKey) {
        updateTableKey(tableKey);
    }

    @Override
    public void setExecuteTime(long executeTime) {
        updateExecuteTime(executeTime);
    }

    @Override
    public void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        List<String> columnNames = columns.getNames();
        for (List<Object> beforeRow : beforeRows) {
            String prepareSql = new InsertBuilder(fetchTableKey().toString(), columnNames, beforeBitmap).prepareSingle();
            String sql = getRealSql(prepareSql, beforeRow);
            logger.info("uidMap: {}, ucsStrategyIdMap: {}", uidMap, ucsStrategyIdMap);

            validation(beforeRow, beforeBitmap, null, null, columns, sql, DmlEnum.INSERT);
        }
    }

    @Override
    public void update(List<List<Object>> beforeRows, Bitmap beforeBitmap, List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns) {
        List<String> columnNames = columns.getNames();
        for (int i = 0; i < beforeRows.size(); i++) {
            Bitmap bitmapOfConditions = Bitmap.union(
                    columns.getBitmapsOfIdentifier().get(0),
                    columns.getLastBitmapOnUpdate()
            );
            List<Object> conditions = beforeBitmap
                    .onBitmap(bitmapOfConditions)
                    .on(beforeRows.get(i));
            String prepareSql = new UpdateBuilder(fetchTableKey().toString(), columnNames, afterBitmap, bitmapOfConditions).prepare();
            List<Object> params = new ArrayList<>(afterRows.get(i));
            params.addAll(conditions);
            String sql = getRealSql(prepareSql, params);
            logger.info("uidMap: {}, ucsStrategyIdMap: {}", uidMap, ucsStrategyIdMap);

            validation(beforeRows.get(i), beforeBitmap, afterRows.get(i), afterBitmap, columns, sql, DmlEnum.UPDATE);
        }
    }

    @Override
    public void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns) {
        List<String> columnNames = columns.getNames();
        for (int i = 0; i < beforeRows.size(); i++) {
            Bitmap bitmapOfConditions = Bitmap.union(
                    columns.getBitmapsOfIdentifier().get(0),
                    columns.getLastBitmapOnUpdate()
            );
            List<Object> conditions = beforeBitmap
                    .onBitmap(bitmapOfConditions)
                    .on(beforeRows.get(i));
            String prepareSql =
                    new DeleteBuilder(fetchTableKey().toString(), columnNames, bitmapOfConditions).prepare();
            String sql = getRealSql(prepareSql, conditions);
            logger.info("uidMap: {}, ucsStrategyIdMap: {}", uidMap, ucsStrategyIdMap);

            validation(beforeRows.get(i), beforeBitmap, null, null, columns, sql, DmlEnum.DELETE);

        }
    }

    @Override
    public void validation(List<Object> beforeRow, Bitmap beforeBitmap, List<Object> afterRow, Bitmap afterBitmap, Columns columns, String sql, DmlEnum dmlEnum) {
        String tableKeyStr = fetchTableKey().toString();
        String uidName = uidMap.get(tableKeyStr);
        if(EXCLUDE_DB.equalsIgnoreCase(tableKeyStr)) {
            totalParseCnt++;
            return;
        }
        if(null == uidName) {
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.validation.unit.notfound.uidname", tableKeyStr);
            totalParseCnt++;
            return;
        }
        String uidValue = getUidValue(beforeRow, beforeBitmap, columns.getNames(), sql);
        Integer ucsStrategyId = ucsStrategyIdMap.get(fetchTableKey().getDatabaseName());
        String ucsStrategyZone = UNKNOWN_ZONE;
        if(null != uidValue && null != ucsStrategyId) {
            RequestContext buildContext = ucsClient.buildRequestContext(uidValue, ucsStrategyId);
            Optional<String> requestZone = buildContext.getRequestZone();
            if(requestZone.isPresent()) {
                ucsStrategyZone = requestZone.get();

                String actualZone = getActualZone();
                if(UNKNOWN_ZONE.equalsIgnoreCase(actualZone)) {
                    logger.info("[Validation][{}]{}, UNLIKELY actualZone not existed", fetchGtid(), sql);
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.validation.unit.notfound.actualzone", fetchGtid());
                } else if (!ucsStrategyZone.equalsIgnoreCase(actualZone)) {
                    logger.info("[Validation][{}]{}, ucsStrategyZone({}) and actualZone({}) not match", fetchGtid(), sql, ucsStrategyZone, actualZone);
                    incorrectResCnt++;
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.validation.unit.incorrect", String.format("%s-%s-%s", tableKeyStr, ucsStrategyZone, actualZone));
                    List<String> beforeValues = dmlEnum.equals(DmlEnum.INSERT) ? null : convertObjectToString(beforeRow);
                    List<String> afterValues = dmlEnum.equals(DmlEnum.DELETE) ? null : (dmlEnum.equals(DmlEnum.INSERT) ? convertObjectToString(beforeRow) : convertObjectToString(afterRow));
                    addValidationWrongResult(sql, ucsStrategyZone, actualZone, columns.getNames(), beforeValues, afterValues);
                } else {
                    logger.info("[Validation][{}]{}, ucsStrategyZone({}) and actualZone({}) are same", fetchGtid(), sql, ucsStrategyZone, actualZone);
                    correctResCnt++;
                }
            } else {
                logger.info("[Validation][{}]{}, UNLIKELY for table:key:value({}:{}:{}) and ucsStrategyId({}), ucsStrategyZone not existed", fetchGtid(), sql, tableKeyStr, uidMap.get(tableKeyStr), uidValue, ucsStrategyId);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.validation.unit.notfound.strategyzone", String.format("%s-%s-%s", tableKeyStr, uidValue, ucsStrategyId));
            }
            totalResCnt++;
        } else {
            logger.info("[Validation][{}]{}, cannot get uidValue({}) or ucsStrategyId({}) for table:key:value({}:{})", fetchGtid(), sql, uidValue, ucsStrategyId, tableKeyStr, uidMap.get(tableKeyStr));
            if(null == uidValue) {
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.validation.unit.notfound.uidvalue", tableKeyStr);
            }
            if(null == ucsStrategyId) {
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.validation.unit.notfound.ucsstrategyid", tableKeyStr);
            }
        }
        totalParseCnt++;
    }

    protected List<String> convertObjectToString(List<Object> objectList) {
        List<String> stringList = Lists.newArrayList();
        for(Object o : objectList) {
            stringList.add(null == o ? null : o.toString());
        }
        return stringList;
    }

    @Override
    public void doInitialize() throws Exception {
        super.doInitialize();
        resultDtos = Lists.newArrayList();
        incorrectResCnt = 0;
        correctResCnt = 0;
        totalResCnt = 0;
        totalParseCnt = 0;
    }

    @Override
    public void doDispose() throws Exception {
        super.doDispose();

        if(resultActivity != null) {
            for(ValidationResultDto dto : resultDtos) {
                if(!resultActivity.report(dto)) {
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.validation.unit.result.discard", fetchTableKey().toString());
                }
            }
        }

        if (metricsActivity != null) {
            metricsActivity.report("unit.route.correct", correctResCnt);
            metricsActivity.report("unit.route.incorrect", incorrectResCnt);
            metricsActivity.report("unit.route.total", totalResCnt);
            metricsActivity.report("unit.route.total.parse", totalParseCnt);
            metricsActivity.report("unit.route.transaction", 1L);
        }

        DefaultEventMonitorHolder.getInstance().logEvent("DRC.validation.unit.transaction", mhaName);
    }

    private void addValidationWrongResult(String sql, String ucsStrategyZone, String actualZone, List<String> columnNames, List<String> beforeValues, List<String> afterValues) {
        ValidationResultDto resultDto = new ValidationResultDto();
        resultDto.setMhaName(mhaName);
        resultDto.setGtid(fetchGtid());
        resultDto.setSql(sql);
        resultDto.setSchemaName(fetchTableKey().getDatabaseName());
        resultDto.setTableName(fetchTableKey().getTableName());
        resultDto.setExpectedDc(ucsStrategyZone.toLowerCase());
        resultDto.setActualDc(actualZone.toLowerCase());
        resultDto.setColumns(columnNames);
        resultDto.setBeforeValues(beforeValues);
        resultDto.setAfterValues(afterValues);
        resultDto.setUidName(uidMap.get(fetchTableKey().toString()));
        resultDto.setUcsStrategyId(ucsStrategyIdMap.get(fetchTableKey().getDatabaseName()));
        resultDto.setExecuteTime(fetchExecuteTime());
        resultDtos.add(resultDto);
    }

    private String getActualZone() {
        for(DBInfo machine : machines) {
            if(fetchGtid().contains(machine.getUuid())) {
                return machine.getIdc();
            }
        }
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.validation.unit.notfound.actual", fetchTableKey().toString());
        return UNKNOWN_ZONE;
    }

    private String getRealSql(String prepareSql, List<Object> values) {
        if(values == null || values.size() == 0) {
            return prepareSql;
        }
        return String.format(prepareSql.replaceAll("\\?", "%s"), values.toArray());
    }

    private String getUidValue(List<Object> rows, Bitmap bitmap, List<String> columnNames, String sql) {
        int valueIndex = getValueIndex(bitmap, columnNames, sql);
        return valueIndex == -1 ? null : rows.get(valueIndex).toString();
    }

    private int getValueIndex(Bitmap bitmap, List<String> columnNames, String sql) {
        logger.debug("[Validation][{}]{}, {}, {}, {}, {}", fetchGtid(), sql, bitmap, columnNames, uidMap, fetchTableKey().toString());
        String uidName = uidMap.get(fetchTableKey().toString());
        if(null == uidName) {
            logger.info("[Validation][{}]{}, UNLIKELY, uidMap {} does not contain table({})", fetchGtid(), sql, uidMap, fetchTableKey().toString());
            return -1;
        }
        int bitMapIndex = caseInsensitiveIndexOf(columnNames, uidName);
        if(bitMapIndex == -1 || !bitmap.get(bitMapIndex)) {
            logger.info("[Validation][{}]{},columnNames {} does not contain uidName({}), or UNLIKELY corresponding bit pos is false in {}", fetchGtid(), sql, columnNames, uidName, bitmap);
            return -1;
        }
        int index = -1;
        for(int i = 0; i <= bitMapIndex; i++){
            if(bitmap.get(i)) {
                index++;
            }
        }
        return index;
    }

    public static int caseInsensitiveIndexOf(List<String> list, String target) {
        if(null == list || list.size() == 0 || target == null) {
            return -1;
        }
        for(int i = 0; i < list.size(); i++) {
            String s = list.get(i);
            if(null != s && s.equalsIgnoreCase(target)) {
                   return i;
            }
        }
        return -1;
    }

    @VisibleForTesting
    public void setUcsClient(UcsClient ucsClient) {
        this.ucsClient = ucsClient;
    }
}
