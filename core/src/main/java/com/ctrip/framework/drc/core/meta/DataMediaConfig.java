package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.monitor.util.IsolateHashCache;
import com.ctrip.framework.drc.core.server.common.filter.column.ColumnsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.DefaultRuleFactory;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.codec.JsonCodec;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @Author limingdong
 * @create 2022/4/27
 */
public class DataMediaConfig {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer concurrency;

    private List<RowsFilterConfig> rowsFilters;

    private List<ColumnsFilterConfig> columnsFilters;

    @JsonIgnore
    private RuleFactory ruleFactory = new DefaultRuleFactory();

    @JsonIgnore
    private String registryKey;

    @JsonIgnore
    private Map<String, RowsFilterConfig> table2Config = Maps.newHashMap();  // regex : RowsFilterConfig

    @JsonIgnore
    private Map<String, ColumnsFilterConfig> table2ColumnConfig = Maps.newHashMap();  // regex : ColumnsFilterConfig

    @JsonIgnore
    private Map<String, AviatorRegexFilter> table2Filter = Maps.newHashMap();  // regex : AviatorRegexFilter

    @JsonIgnore
    private Map<String, AviatorRegexFilter> table2ColumnFilter = Maps.newHashMap();  // regex : AviatorRegexFilter for columns filter

    @JsonIgnore
    private Map<String, RowsFilterRule> table2Rule = Maps.newHashMap();  // regex : RowsFilterRule

    @JsonIgnore
    private Map<String, ColumnsFilterRule> table2ColumnRule = Maps.newHashMap();  // regex : ColumnsFilterRule

    @JsonIgnore
    private IsolateHashCache<String, Optional<RowsFilterRule>> matchResult = new IsolateHashCache<>(5000, 16, 4);  // tableName : RowsFilterRuleWrapper

    @JsonIgnore
    private IsolateHashCache<String, Optional<ColumnsFilterRule>> matchColumnsResult = new IsolateHashCache<>(5000, 16, 4);  // tableName : ColumnsFilterRuleWrapper

    public List<RowsFilterConfig> getRowsFilters() {
        return rowsFilters;
    }

    public void setRowsFilters(List<RowsFilterConfig> rowsFilters) {
        this.rowsFilters = rowsFilters;
    }

    public List<ColumnsFilterConfig> getColumnsFilters() {
        return columnsFilters;
    }

    public void setColumnsFilters(List<ColumnsFilterConfig> columnsFilters) {
        this.columnsFilters = columnsFilters;
    }

    public void setRegistryKey(String registryKey) {
        this.registryKey = registryKey;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public DataMediaConfig() {
    }

    public static DataMediaConfig from(String registryKey, String properties) throws Exception {
        DataMediaConfig dataMediaConfig;
        if (StringUtils.isNotBlank(properties)) {
            dataMediaConfig = JsonCodec.INSTANCE.decode(properties, DataMediaConfig.class);
        } else {
            dataMediaConfig = new DataMediaConfig();
        }
        dataMediaConfig.setRegistryKey(registryKey);
        dataMediaConfig.parse();
        return dataMediaConfig;
    }

    private void parse() throws Exception {
        if (valid(rowsFilters)) {
            for (RowsFilterConfig rowsFilterConfig : rowsFilters) {
                rowsFilterConfig.setRegistryKey(registryKey);
                String tableRegex = rowsFilterConfig.getTables().trim().toLowerCase();
                table2Config.put(tableRegex, rowsFilterConfig);
                table2Filter.put(tableRegex, new AviatorRegexFilter(tableRegex));
                RowsFilterRule<List<List<Object>>> rowsFilterRule = ruleFactory.createRowsFilterRule(rowsFilterConfig);
                table2Rule.put(tableRegex, rowsFilterRule);
            }
        }

        if (valid(columnsFilters)) {
            for (ColumnsFilterConfig columnsFilterConfig : columnsFilters) {
                columnsFilterConfig.setRegistryKey(registryKey);
                String tableRegex = columnsFilterConfig.getTables().trim().toLowerCase();
                table2ColumnConfig.put(tableRegex, columnsFilterConfig);
                table2ColumnFilter.put(tableRegex, new AviatorRegexFilter(tableRegex));
                ColumnsFilterRule columnsFilterRule = ruleFactory.createColumnsFilterRule(columnsFilterConfig);
                table2ColumnRule.put(tableRegex, columnsFilterRule);
            }
        }
    }

    public boolean shouldFilterRows() {
        return valid(rowsFilters) && rowsFilters.stream().anyMatch(RowsFilterConfig::shouldFilterRows);
    }

    public boolean shouldFilterColumns() {
        return valid(columnsFilters) && columnsFilters.stream().anyMatch(ColumnsFilterConfig::shouldFilterColumns);
    }

    /**
     * return null if not valid
     *
     * @param tableName
     * @return
     */
    public Optional<RowsFilterRule> getRowsFilterRule(String tableName) {
        Optional<RowsFilterRule> optional = Optional.empty();
        if (valid(rowsFilters)) {
            optional = matchResult.getIfPresent(tableName);
            if (optional == null) {
                RowsFilterRule rowsFilterRule = null;
                for (Map.Entry<String, RowsFilterConfig> entry : table2Config.entrySet()) {
                    if (entry.getValue().shouldFilterRows() && table2Filter.get(entry.getKey()).filter(tableName)) {
                        rowsFilterRule = table2Rule.get(entry.getKey());
                        break;
                    }
                }
                optional = Optional.ofNullable(rowsFilterRule);
                matchResult.put(tableName, optional);
            }
        }

        return optional;
    }

    public Optional<ColumnsFilterRule> getColumnsFilterRule(String tableName) {
        Optional<ColumnsFilterRule> optional = Optional.empty();
        if (valid(columnsFilters)) {
            optional = matchColumnsResult.getIfPresent(tableName);
            if (optional == null) {
                ColumnsFilterRule columnsFilterRule = null;
                for (Map.Entry<String, ColumnsFilterConfig> entry : table2ColumnConfig.entrySet()) {
                    if (table2ColumnFilter.get(entry.getKey()).filter(tableName)) {
                        columnsFilterRule = table2ColumnRule.get(entry.getKey());
                        break;
                    }
                }
                optional = Optional.ofNullable(columnsFilterRule);
                matchColumnsResult.put(tableName, optional);
            }
        }

        return optional;
    }

    public Integer getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    private <E> boolean valid(Collection<E> filters) {
        return filters != null && !filters.isEmpty();
    }
}
