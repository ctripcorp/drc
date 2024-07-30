package com.ctrip.framework.drc.core.server.common.filter.row.soa;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent.Row;
import com.ctrip.framework.drc.core.exception.DrcException;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig.SoaIdentifier;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.core.server.common.filter.row.AbstractRowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult.Status;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.service.CustomSoaService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName SoaCustomRowsFilterRule
 * @Author haodongPan
 * @Date 2024/7/15 16:05
 * @Version: $
 * http://conf.ctripcorp.com/pages/viewpage.action?pageId=2628423081
 */
public class CustomSoaRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<Row>> {
    
    private CustomSoaService customSoaService = ServicesUtil.getCustomSoaService();
    
    protected int serviceCode;
    protected String serviceName;
    
    public CustomSoaRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
        String context = rowsFilterConfig.getConfigs().getParameterList().get(0).getContext();
        SoaIdentifier soaIdentifier = JsonUtils.fromJson(context, SoaIdentifier.class);
        serviceCode = soaIdentifier.getCode();
        serviceName = soaIdentifier.getName();
    }

    @Override
    protected List<AbstractRowsEvent.Row> doFilterRows(AbstractRowsEvent rowsEvent, RowsFilterContext rowFilterContext, LinkedHashMap<String, Integer> indices) throws Exception {
        List<AbstractRowsEvent.Row> result = Lists.newArrayList();
        List<List<Object>> values = getValues(rowsEvent);
        List<AbstractRowsEvent.Row> rows = rowsEvent.getRows();

        String fullTableName = rowFilterContext.getDrcTableMapLogEvent().getSchemaNameDotTableName();
        if (CollectionUtils.isEmpty(indices) ) {
            throw new DrcException(String.format("[CustomSoaRowsFilter] filter without matching column for %s:%s", fullTableName, registryKey));
        }
        if (indices.size() != 1) {
            throw new DrcException(String.format("[CustomSoaRowsFilter] config multi column not support %s:%s", fullTableName, registryKey));
        }

        for (Entry<String, Integer> columnNameIndex : indices.entrySet()) { 
            // actually only one loop
            String columnName = columnNameIndex.getKey();
            Integer columnIndex = columnNameIndex.getValue();
            for (int i = 0; i < values.size(); ++i) {
                Object filed = values.get(i).get(columnIndex);
                String filedString = String.valueOf(filed);
                CustomSoaCacheKey cacheKey = new CustomSoaCacheKey(serviceCode, fullTableName,filedString);
                Status cache = rowFilterContext.get(cacheKey);
                if (cache == null) {
                    CustomSoaRowFilterContext requestCtx = fetchRequestCtx(columnName, filedString, rowFilterContext);
                    cache = DefaultTransactionMonitorHolder.getInstance().logTransaction(
                            "DRC.replicator.rows.filter.soa." + serviceCode, 
                            registryKey, 
                            () -> customSoaService.filter(serviceCode,serviceName,requestCtx)
                    );
                    rowFilterContext.putIfAbsent(cacheKey, cache);
                } else {
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.rows.filter.soa.cache", registryKey);
                }
                if (cache.noRowFiltered()) {
                    result.add(rows.get(i));
                }
            }
        }
        return result;
    }
    
    
    private CustomSoaRowFilterContext fetchRequestCtx(String filedName,String field,RowsFilterContext rowsFilterCtx) {
        return new CustomSoaRowFilterContext(
                rowsFilterCtx.getDrcTableMapLogEvent().getSchemaName().toLowerCase(),
                rowsFilterCtx.getDrcTableMapLogEvent().getTableName().toLowerCase(),
                filedName,
                field,
                rowsFilterCtx.getSrcRegion(), 
                rowsFilterCtx.getDstRegion()
        );
    }
    
    @VisibleForTesting
    protected void setCustomSoaService(CustomSoaService customSoaService) {
        this.customSoaService = customSoaService;
    }

    public static class CustomSoaCacheKey {
        private int code;
        private String tableName;
        private String columnValue;
        
        public CustomSoaCacheKey(int code, String tableName, String columnValue) {
            this.code = code;
            this.tableName = tableName;
            this.columnValue = columnValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CustomSoaCacheKey cacheKey = (CustomSoaCacheKey) o;
            return code == cacheKey.code && Objects.equals(tableName, cacheKey.tableName) && Objects.equals(
                    columnValue, cacheKey.columnValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(code, tableName, columnValue);
        }
    }
}
