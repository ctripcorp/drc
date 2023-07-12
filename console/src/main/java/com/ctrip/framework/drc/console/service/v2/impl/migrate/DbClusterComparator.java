package com.ctrip.framework.drc.console.service.v2.impl.migrate;

import static com.ctrip.framework.drc.core.service.utils.Constants.COMMA;

import com.ctrip.framework.drc.console.service.DrcBuildService;
import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.fetcher.resource.transformer.TransformerHelper;
import com.ctrip.xpipe.codec.JsonCodec;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName DbClusterComparator
 * @Author haodongPan
 * @Date 2023/7/10 16:37
 * @Version: $
 */
public class DbClusterComparator implements Callable<String> {
    
    private final DbCluster oldDbCluster;
    
    private final DbCluster newDbCluster;
    
    private final DrcBuildService drcBuildService;
    
    private final StringBuilder recorder;
    
    private final boolean costTimeTrace;
    
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public DbClusterComparator(DbCluster oldDbCluster, DbCluster newDbCluster,
            DrcBuildService drcBuildService,  boolean costTimeTrace) {
        this.oldDbCluster = oldDbCluster;
        this.newDbCluster = newDbCluster;
        this.drcBuildService = drcBuildService;
        this.recorder = new StringBuilder();
        this.costTimeTrace = costTimeTrace;
    }

    @Override
    public String call() throws Exception {
        try {
            long start = System.currentTimeMillis();
            recorder.append("\n[CompareDbCluster]:").append(oldDbCluster.getId());
            if (null == newDbCluster) {
                return recorder.append("\nnewDbCluster is empty").toString();
            }
            
            boolean dbsEquals = oldDbCluster.getDbs().equals(newDbCluster.getDbs());
            boolean replicatorsEquals = oldDbCluster.getReplicators().equals(newDbCluster.getReplicators());

            if (!dbsEquals) {
                recorder.append("\ndbs is not equal!");
            }
            if (!replicatorsEquals) {
                recorder.append("\nreplicators is not equal!");
            }

            // compare Applier
            List<Applier> oldAppliers = oldDbCluster.getAppliers();
            List<Applier> newAppliers = newDbCluster.getAppliers();
            if (oldAppliers.size() != newAppliers.size()) {
                recorder.append("\n Appliers size is not equal!");
            }
            for (Applier oldApplier : oldAppliers) {
                Applier newApplier = newAppliers.stream().filter(applier -> applier.getIp().equals(oldApplier.getIp()) &&
                        applier.getTargetMhaName().equals(oldApplier.getTargetMhaName())
                ).findFirst().orElse(null);
                
                recorder.append("\n[CompareApplier]:").append(oldApplier.getTargetMhaName()).append("->")
                        .append(oldDbCluster.getMhaName()).append("-").append(oldApplier.getIp());
                if (newApplier == null) {
                    recorder.append("\n newApplier is empty!");
                    continue;
                }
                if (!equalsExcludeConfig(oldApplier, newApplier)) {
                    recorder.append("\n ApplierExcludeConfig is not equals!");
                }
                
                long applierStart = System.currentTimeMillis();
                
                String oldNameFilter = oldApplier.getNameFilter();
                String newNameFilter = newApplier.getNameFilter();
                if (!compareNameFilter(oldApplier.getTargetMhaName(), oldDbCluster.getMhaName(), oldNameFilter, newNameFilter)) {
                    recorder.append("\n compareNameFilter is not equals!");
                }
                
                long compareNameFilter = System.currentTimeMillis();
                if (costTimeTrace) {
                    recorder.append("\ncompareNameFilter end").append("cost ms:").append(compareNameFilter - applierStart);
                }
                
                String oldNameMapping = oldApplier.getNameMapping();
                String newNameMapping = newApplier.getNameMapping();
                if (!compareNameMapping(oldNameMapping, newNameMapping)) {
                    recorder.append("\n compareNameMapping is not equals!");
                }
                
                long compareNameMapping = System.currentTimeMillis();
                if (costTimeTrace) {
                    recorder.append("\ncompareNameMapping end").append("cost ms:").append(compareNameMapping-compareNameFilter);
                }

                String oldProperties = oldApplier.getProperties();
                String newProperties = newApplier.getProperties();
                if (!compareProperties(oldApplier.getTargetMhaName(), oldDbCluster.getMhaName(), oldProperties, newProperties)) {
                    recorder.append("\n compareProperties is not equals!");
                }

                long compareProperties = System.currentTimeMillis();
                if (costTimeTrace) {
                    recorder.append("\ncompareProperties end").append("cost ms:").append(compareProperties - compareNameMapping);
                }
            }

            // compare Messenger
            List<Messenger> oldMessengers = oldDbCluster.getMessengers();
            List<Messenger> newMessengers = newDbCluster.getMessengers();
            if (oldMessengers.size() != newMessengers.size()) {
                recorder.append("\n Messengers size is not equal!");
            }
            for (Messenger oldMessenger : oldMessengers) {
                Messenger newMessenger = newMessengers.stream().
                        filter(messenger -> messenger.getIp().equals(oldMessenger.getIp())).findFirst().orElse(null);
                recorder.append("\n[CompareMessenger]:").append(oldDbCluster.getMhaName()).append("-").append(oldMessenger.getIp());
                if (newMessenger == null) {
                    recorder.append("\n newMessenger is empty!");
                    continue;
                }

                if (!equalsExcludeConfig(oldMessenger, newMessenger)) {
                    recorder.append("\n MessengerExcludeConfig is not equals!");
                }

                long messengerStart = System.currentTimeMillis();
                
                String oldNameFilter = oldMessenger.getNameFilter();
                String newNameFilter = newMessenger.getNameFilter();
                if (!compareNameFilter(oldDbCluster.getMhaName(),null, oldNameFilter, newNameFilter)) {
                    recorder.append("\n compareNameFilter is not equals!");
                }
                
                long compareNameFilter = System.currentTimeMillis();
                if (costTimeTrace) {
                    recorder.append("\ncompareNameFilter end").append("cost ms:").append(compareNameFilter - messengerStart);
                }

                String oldProperties = oldMessenger.getProperties();
                String newProperties = newMessenger.getProperties();
                if (!compareMessengerConfigs(oldDbCluster.getMhaName(), oldProperties, newProperties)) {
                    recorder.append("\n compareMessengerConfigs is not equals!");
                }

                long compareProperties = System.currentTimeMillis();
                if (costTimeTrace) {
                    recorder.append("\ncompareMessengerConfigs end").append("cost ms:").append(compareProperties - compareNameFilter);
                }
            }
            
            long end = System.currentTimeMillis();
            recorder.append("\ncompare DbCluster end").append("cost ms:").append(end-start);
            
        } catch (Exception e) {
            logger.error("[[tag=xmlCompare]] compare DbCluster:{} fail",oldDbCluster.getId(),e);
            recorder.append("\n[[tag=xmlCompare]] compare DbCluster fail,").append(oldDbCluster.getId());
        }
        return recorder.toString();
    }

    private boolean compareNameFilter(String srcMha,String destMha,String oldNameFilter, String newNameFilter) {
        List<String> oldTables = drcBuildService.queryTablesWithNameFilter(srcMha, oldNameFilter);
        List<String> newTables = drcBuildService.queryTablesWithNameFilter(srcMha, newNameFilter);

        if (CollectionUtils.isEmpty(oldTables)) {
            logger.warn("[[tag=xmlCompare]] queryMha:{}-{} match table is empty",srcMha, oldNameFilter);
            oldTables = drcBuildService.queryTablesWithNameFilter(destMha, oldNameFilter);
        }
        if (CollectionUtils.isEmpty(newTables)) {
            logger.warn("[[tag=xmlCompare]] queryMha:{}-{} match table is empty",srcMha, newNameFilter);
            newTables = drcBuildService.queryTablesWithNameFilter(destMha, newNameFilter);
        }
        
        if (CollectionUtils.isEmpty(oldTables) || CollectionUtils.isEmpty(newTables) || oldTables.size() != newTables.size()) {
            logger.warn("[[tag=xmlCompare]] queryMha:{}-{}-{} match table size is {}",srcMha, destMha, newNameFilter,CollectionUtils.isEmpty(oldTables) ? 0 :oldTables.size());
            logger.warn("[[tag=xmlCompare]] queryMha:{}-{}-{} match table size is {}",srcMha, destMha, newNameFilter,CollectionUtils.isEmpty(newTables) ? 0 :newTables.size());
            return false;
        }

        Collections.sort(oldTables);
        Collections.sort(newTables);
        return oldTables.equals(newTables);
    }

    private boolean compareRowsFilters(String srcMha,String destMha,List<RowsFilterConfig> oldRowsFilters,
            List<RowsFilterConfig> newRowsFilters) {
        if (CollectionUtils.isEmpty(oldRowsFilters) && CollectionUtils.isEmpty(newRowsFilters)) {
            return true;
        }

        Map<String, List<String>> oldConfigsGroup = groupTableByRowsFilter(oldRowsFilters);
        Map<String, List<String>> newConfigsGroup = groupTableByRowsFilter(newRowsFilters);

        if (oldConfigsGroup.size() != newConfigsGroup.size()) {
            recorder.append("\n rowsFilter size is not equals!");
            return false;
        }
        for (Entry<String, List<String>> entry : oldConfigsGroup.entrySet()) {
            String configs = entry.getKey();
            List<String> oldTables = entry.getValue();
            List<String> newTables = newConfigsGroup.getOrDefault(configs, null);

            if (null == newTables) {
                recorder.append("\n newMetaTables is empty for rowsFilter: ").append(configs);
                return false;
            }
            if (!compareNameFilter(srcMha, destMha, StringUtils.join(oldTables, COMMA), StringUtils.join(newTables, COMMA))) {
                recorder.append("\n table size is not equals for rowsFilter: ").append(configs);
                return false;
            }
        }
        return true;

    }

    private boolean compareColumnsFilters(String srcMha,String destMha,List<ColumnsFilterConfig> oldColumnsFilters,
            List<ColumnsFilterConfig> newColumnsFilters) {
        if (CollectionUtils.isEmpty(oldColumnsFilters) && CollectionUtils.isEmpty(newColumnsFilters)) {
            return true;
        }

        Map<String, List<String>> oldTablesGroup = groupTableByColumnFilter(oldColumnsFilters);
        Map<String, List<String>> newTablesGroup = groupTableByColumnFilter(newColumnsFilters);

        if (oldTablesGroup.size() != newTablesGroup.size()) {
            recorder.append("\n columnsFilter size is not equals!");
            return false;
        }
        for (Entry<String, List<String>> entry : oldTablesGroup.entrySet()) {
            String configs = entry.getKey();
            List<String> oldTables = entry.getValue();
            List<String> newTables = newTablesGroup.getOrDefault(configs, null);

            if (null == newTables) {
                recorder.append("\n newMetaTables is empty for columnsFilter: ").append(configs);
                return false;
            }
            if (!compareNameFilter(srcMha, destMha, StringUtils.join(oldTables, COMMA), StringUtils.join(newTables, COMMA))) {
                recorder.append("\n table size is not equals for columnsFilter: ").append(configs);
                return false;
            }
        }
        return true;
    }



    private boolean compareMessengerConfigs(String mha,String oldProperties, String newProperties) {
        if (StringUtils.isBlank(oldProperties) && StringUtils.isBlank(newProperties)) {
            return true;
        }
        if (StringUtils.isBlank(oldProperties) || StringUtils.isBlank(newProperties)) {
            recorder.append("\ncompareMessengerConfigs is not equal, one side is empty!");
            return false;
        }
        MessengerProperties oldMessengerConfigs = JsonCodec.INSTANCE.decode(newProperties, MessengerProperties.class);
        MessengerProperties newMessengerConfigs = JsonCodec.INSTANCE.decode(oldProperties, MessengerProperties.class);

        List<MqConfig> oldMqConfigs = oldMessengerConfigs.getMqConfigs();
        List<MqConfig> newMqConfigs = newMessengerConfigs.getMqConfigs();

        return compareMqConfig(mha,oldMqConfigs,newMqConfigs);

    }

    private boolean compareMqConfig(String mha, List<MqConfig> oldMqConfigs, List<MqConfig> newMqConfigs) {
        if (CollectionUtils.isEmpty(oldMqConfigs) && CollectionUtils.isEmpty(newMqConfigs)) {
            return true;
        }

        Map<String, List<String>> oldConfigsGroup = groupTableByMqConfig(oldMqConfigs);
        Map<String, List<String>> newConfigsGroup = groupTableByMqConfig(newMqConfigs);

        if (oldConfigsGroup.size() != newConfigsGroup.size()) {
            recorder.append("\n mqConfigs size is not equals!");
            return false;
        }
        for (Entry<String, List<String>> entry : oldConfigsGroup.entrySet()) {
            String configs = entry.getKey();
            List<String> oldTables = entry.getValue();
            List<String> newTables = newConfigsGroup.getOrDefault(configs, null);

            if (null == newTables) {
                recorder.append("\n newMetaTables is empty for mqConfig: ").append(configs);
                return false;
            }
            if (!compareNameFilter(mha, null, StringUtils.join(oldTables, COMMA), StringUtils.join(newTables, COMMA))) {
                recorder.append("\n table size is not equals for mqConfig: ").append(configs);
                return false;
            }
        }
        return true;
    }



    private boolean compareProperties(String srcMha,String destMha,String oldProperties, String newProperties) {
        if (StringUtils.isBlank(oldProperties) && StringUtils.isBlank(newProperties)) {
            return true;
        }
        if (StringUtils.isBlank(oldProperties) || StringUtils.isBlank(newProperties)) {
            recorder.append("\ncompareProperties is not equal, one side is empty!");
            return false;
        }

        DataMediaConfig oldDataMediaConfig = JsonCodec.INSTANCE.decode(oldProperties, DataMediaConfig.class);
        DataMediaConfig newDataMediaConfig = JsonCodec.INSTANCE.decode(newProperties, DataMediaConfig.class);

        List<RowsFilterConfig> oldRowsFilters = oldDataMediaConfig.getRowsFilters();
        List<RowsFilterConfig> newRowsFilters = newDataMediaConfig.getRowsFilters();

        List<ColumnsFilterConfig> oldColumnsFilters = oldDataMediaConfig.getColumnsFilters();
        List<ColumnsFilterConfig> newColumnsFilters = newDataMediaConfig.getColumnsFilters();

        return compareRowsFilters(srcMha, destMha, oldRowsFilters, newRowsFilters)
                && compareColumnsFilters(srcMha,destMha,oldColumnsFilters,newColumnsFilters);

    }


    // k: mode + configs ,v: a group of table
    private Map<String,List<String>> groupTableByRowsFilter (List<RowsFilterConfig> rowsFilterConfigs) {
        Map<String,List<String>> res = Maps.newHashMap();
        for (RowsFilterConfig rowsFilterConfig : rowsFilterConfigs) {
            String key = rowsFilterConfig.getMode() + rowsFilterConfig.getConfigs().toString();
            List<String> group = res.getOrDefault(key,Lists.newArrayList());
            if (CollectionUtils.isEmpty(group)) {
                res.put(key,group);
            }
            group.add(rowsFilterConfig.getTables());

        }
        return res;
    }

    // k: mode + columns ,v: a group of table
    private Map<String,List<String>> groupTableByColumnFilter (List<ColumnsFilterConfig> columnsFilters) {
        Map<String,List<String>> res = Maps.newHashMap();
        for (ColumnsFilterConfig config : columnsFilters) {
            String key = config.getMode() + config.getColumns();
            List<String> group = res.getOrDefault(key, Lists.newArrayList());
            if (CollectionUtils.isEmpty(group)) {
                res.put(key,group);
            }
            group.add(config.getTables());
        }
        return res;
    }

    // k: configs except table ,v: a group of table
    private Map<String,List<String>> groupTableByMqConfig (List<MqConfig> mqConfigs) {
        Map<String,List<String>> res = Maps.newHashMap();
        for (MqConfig mqConfig : mqConfigs) {
            String key = mqConfig.getTopic() + mqConfig.getMqType() + mqConfig.getSerialization()
                    + mqConfig.isPersistent() + mqConfig.getOrderKey() + mqConfig.getDelayTime();
            List<String> group = res.getOrDefault(key,Lists.newArrayList());
            if (CollectionUtils.isEmpty(group)) {
                group.add(mqConfig.getTable());
                res.put(key,group);
            } else {
                group.add(mqConfig.getTable());
            }
        }
        return res;
    }

    private boolean compareNameMapping(String oldNameMapping, String newNameMapping) {
        if (StringUtils.isBlank(oldNameMapping) && StringUtils.isBlank(newNameMapping)) {
            return true;
        }
        if (StringUtils.isBlank(oldNameMapping) || StringUtils.isBlank(newNameMapping)) {
            recorder.append("\nnameMapping is not equal, one side is empty!");
            return false;
        }
        
        Map<String, String> oldNameMappingInfo = parseNameMapping(oldNameMapping);
        Map<String, String> newNameMappingInfo = parseNameMapping(newNameMapping);
        if (oldNameMappingInfo.size() != newNameMappingInfo.size()) {
            recorder.append("\nnameMapping is not equal, mapping size not equal!");
            return false;
        }

        for (Entry<String, String> entry : oldNameMappingInfo.entrySet()) {
            String srcName = entry.getKey();
            String oldDestName = entry.getValue();
            String newDestName = newNameMappingInfo.getOrDefault(srcName, null);
            if (null == newDestName || !newDestName.equals(oldDestName)) {
                recorder.append("\nnameMapping is not equal").append("srcTableName:").append(srcName);
                return false;
            }
        }
        return true;
        
    }
    
    private Map<String, String> parseNameMapping(String nameMapping) {
        Map<String, String> nameMap = Maps.newLinkedHashMap();
        if (StringUtils.isBlank(nameMapping)) {
            return nameMap;
        }
        String[] names = StringUtils.split(nameMapping, ';');

        for (String name : names) {
            String[] namePair = StringUtils.split(name, ',');
            String sourceName = namePair[0];
            String targetName = namePair[1];

            if (sourceName.contains("[")) {
                List<String> sourceNames = parseNameMode(sourceName);
                List<String> targetNames = parseNameMode(targetName);
                if (sourceNames.size() != targetNames.size()) {
                    logger.error("source name: {} and target name: {} size are not same", sourceNames, targetNames);
                    throw new RuntimeException("source and target name size are not same");
                }
                for (int i = 0; i < sourceNames.size(); i++) {
                    nameMap.put(sourceNames.get(i), targetNames.get(i));
                }
            } else {
                nameMap.put(parseSingleName(sourceName), parseSingleName(targetName));
            }
        }
        logger.info("init name mapping success, size is: {}, content is: {}", nameMap.size(), nameMap);
        return nameMap;
    }

    private List<String> parseNameMode(String name) {
        String[] names = StringUtils.split(name, '.');
        String schemaMode = names[0];
        String tableMode = names[1];

        List<String> schemas = TransformerHelper.parseMode(schemaMode);
        List<String> tables = TransformerHelper.parseMode(tableMode);

        List<String> schemaDotTables = Lists.newArrayList();
        for (String schema : schemas) {
            for (String table : tables) {
                schemaDotTables.add("`" + schema + "`.`" + table + "`");
            }
        }
        return schemaDotTables;
    }

    private String parseSingleName(String name) {
        String[] names = StringUtils.split(name, '.');
        String schema = names[0];
        String table = names[1];
        return "`" + schema + "`.`" + table + "`";
    }
    

    private boolean equalsExcludeConfig(Applier a1, Applier a2) {
        return a1.getIp().equals(a2.getIp())
                && a1.getPort().equals(a2.getPort())
                && a1.getTargetRegion().equals(a2.getTargetRegion())
                && a1.getTargetIdc().equals(a2.getTargetIdc())
                && a1.getTargetMhaName().equals(a2.getTargetMhaName())
                && a1.getTargetName().equals(a2.getTargetName())
                && a1.getMaster()== (a2.getMaster())
                && a1.getApplyMode().equals(a2.getApplyMode())
                && StringUtils.equals(a1.getIncludedDbs(),a2.getIncludedDbs()); // could be null
    }

    private boolean equalsExcludeConfig(Messenger m1, Messenger m2) {
        return m1.getIp().equals(m2.getIp())
                && m1.getPort().equals(m2.getPort())
                && m1.getMaster()== (m2.getMaster())
                && StringUtils.equals(m1.getGtidExecuted(),m2.getGtidExecuted());
    }

    
}
