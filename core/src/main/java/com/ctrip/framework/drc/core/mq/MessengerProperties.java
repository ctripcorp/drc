package com.ctrip.framework.drc.core.mq;

import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.codec.JsonCodec;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_DELAY_MONITOR_NAME;

/**
 * @ClassName MessengerProperties
 * @Author haodongPan
 * @Date 2022/10/9 11:08
 * @Version: $
 */
public class MessengerProperties {

    private String nameFilter;

    private DataMediaConfig dataMediaConfig;

    private List<MqConfig> mqConfigs;

    @JsonIgnore
    private Map<String, List<MqConfig>> regex2Configs = Maps.newConcurrentMap();  // regex : mqConfigs

    @JsonIgnore
    private Map<String, AviatorRegexFilter> regex2Filter = Maps.newConcurrentMap();  // regex : AviatorRegexFilter

    @JsonIgnore
    private Map<String, List<Producer>> regex2Producers = Maps.newConcurrentMap();  // regex : Producers

    @JsonIgnore
    private Map<String, List<Producer>> tableName2Producers = Maps.newConcurrentMap(); // tableName :  Producers

    public String getNameFilter() {
        return nameFilter;
    }

    public void setNameFilter(String nameFilter) {
        this.nameFilter = nameFilter;
    }

    public DataMediaConfig getDataMediaConfig() {
        return dataMediaConfig;
    }

    public void setDataMediaConfig(DataMediaConfig dataMediaConfig) {
        this.dataMediaConfig = dataMediaConfig;
    }

    public List<MqConfig> getMqConfigs() {
        return mqConfigs;
    }

    public void setMqConfigs(List<MqConfig> mqConfigs) {
        this.mqConfigs = mqConfigs;
    }

    public Map<String, List<Producer>> getRegex2Producers() {
        return regex2Producers;
    }

    public static MessengerProperties from(String properties) throws Exception {
        MessengerProperties messengerProperties;
        if (StringUtils.isNotBlank(properties)) {
            messengerProperties = JsonCodec.INSTANCE.decode(properties, MessengerProperties.class);
        } else {
            messengerProperties = new MessengerProperties();
        }
        messengerProperties.parse();
        return messengerProperties;
    }

    private void parse() throws Exception {
        if (mqConfigs == null) {
            return;
        }
        for (MqConfig mqConfig : mqConfigs) {
            String tableRegex = mqConfig.getTable().trim().toLowerCase();
            List<MqConfig> mqConfigs = regex2Configs.get(tableRegex);
            if (mqConfigs == null) {
                mqConfigs = Lists.newArrayList(mqConfig);
            } else {
                mqConfigs.add(mqConfig);
            }
            regex2Configs.put(tableRegex, mqConfigs);

            AviatorRegexFilter filter = regex2Filter.get(tableRegex);
            if (filter == null) {
                regex2Filter.put(tableRegex, new AviatorRegexFilter(tableRegex));
            }
        }
    }

    public List<Producer> getProducers(String tableName) {
        String formatTableName = tableName.toLowerCase();
        List<Producer> tableNameProducers = tableName2Producers.get(formatTableName);
        if (tableNameProducers != null) {
            return tableNameProducers;
        }

        if (DRC_DELAY_MONITOR_NAME.equalsIgnoreCase(formatTableName)) {
            return DelayMessageProducer.getInstance();
        }

        List<Producer> toCreateProducers = Lists.newArrayList();
        for (Map.Entry<String, List<MqConfig>> entry : regex2Configs.entrySet()) {
            String tableRegex = entry.getKey().trim().toLowerCase();
            if (regex2Filter.get(tableRegex).filter(formatTableName)) {
                synchronized (this) {
                    tableNameProducers = tableName2Producers.get(formatTableName);
                    if (tableNameProducers != null) {
                        return tableNameProducers;
                    }
                    List<Producer> regexProducers = regex2Producers.get(tableRegex);
                    if (regexProducers == null) {
                        for (MqConfig mqConfig : entry.getValue()) {
                            Producer producer = DefaultProducerFactoryHolder.getInstance().createProducer(mqConfig);
                            toCreateProducers.add(producer);
                        }
                        regex2Producers.put(tableRegex, toCreateProducers);
                        tableName2Producers.put(formatTableName, toCreateProducers);
                        return toCreateProducers;
                    } else {
                        tableName2Producers.put(formatTableName, regexProducers);
                        return regexProducers;
                    }
                }
            }
        }
        return toCreateProducers;
    }
}
