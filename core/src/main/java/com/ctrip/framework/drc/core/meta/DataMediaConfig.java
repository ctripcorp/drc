package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.monitor.util.IsolateHashCache;
import com.ctrip.framework.drc.core.server.common.filter.row.DefaultRuleFactory;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory;
import com.ctrip.xpipe.codec.JsonCodec;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * @Author limingdong
 * @create 2022/4/27
 */
public class DataMediaConfig {

    private List<RowsFilterConfig> rowsFilters;

    @JsonIgnore
    private RuleFactory ruleFactory = new DefaultRuleFactory();

    @JsonIgnore
    private String registryKey;

    @JsonIgnore
    private Map<String, RowsFilterConfig> table2Config = Maps.newHashMap();  // regex : RowsFilterConfig

    @JsonIgnore
    private Map<String, RowsFilterRule> table2Rule = Maps.newHashMap();  // regex : RowsFilterRule

    @JsonIgnore
    private IsolateHashCache<String, Optional<RowsFilterRule>> matchResult = new IsolateHashCache<>(5000, 16, 4);  // tableName : RowsFilterRuleWrapper

    public List<RowsFilterConfig> getRowsFilters() {
        return rowsFilters;
    }

    public void setRowsFilters(List<RowsFilterConfig> rowsFilters) {
        this.rowsFilters = rowsFilters;
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
        if (valid()) {
            for (RowsFilterConfig rowsFilterConfig : rowsFilters) {
                rowsFilterConfig.setRegistryKey(registryKey);
                String tableRegex = rowsFilterConfig.getTables().trim().toLowerCase();
                table2Config.put(tableRegex, rowsFilterConfig);
                RowsFilterRule<List<List<Object>>> rowsFilterRule = ruleFactory.createRowsFilterRule(rowsFilterConfig);
                table2Rule.put(tableRegex, rowsFilterRule);
            }
        }
    }

    public boolean shouldFilterRows() {
        return valid() ? rowsFilters.stream().anyMatch(rowsFilterConfig -> rowsFilterConfig.shouldFilterRows()) : false;
    }

    /**
     * return null if not valid
     * @param tableName
     * @return
     */
    public Optional<RowsFilterRule>  getRowsFilterRule(String tableName) {
        Optional<RowsFilterRule> optional = Optional.empty();
        if (valid()) {
            optional = matchResult.getIfPresent(tableName);
            if (optional == null) {
                RowsFilterRule rowsFilterRule = null;
                for (Map.Entry<String, RowsFilterConfig> entry : table2Config.entrySet()) {
                    if (entry.getValue().shouldFilterRows() && Pattern.matches(entry.getKey(), tableName)) {
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

    private boolean valid() {
        return rowsFilters != null && !rowsFilters.isEmpty();
    }
}
