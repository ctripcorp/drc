package com.ctrip.framework.drc.console.service.v2.impl.migrate;

import static com.ctrip.framework.drc.core.service.utils.Constants.COMMA;
import static com.ctrip.framework.drc.core.service.utils.Constants.ESCAPE_DOT_REGEX;
import static com.ctrip.framework.drc.core.service.utils.Constants.SEMICOLON;

import com.ctrip.framework.drc.console.service.DrcBuildService;
import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
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
    
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public DbClusterComparator( DbCluster oldDbCluster,
            DbCluster newDbCluster, DrcBuildService drcBuildService) {
        this.oldDbCluster = oldDbCluster;
        this.newDbCluster = newDbCluster;
        this.drcBuildService = drcBuildService;
        this.recorder = new StringBuilder();
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
                recorder.append("\ncompareNameFilter end").append("cost ms:").append(compareNameFilter-applierStart);
                
                String oldNameMapping = oldApplier.getNameMapping();
                String newNameMapping = newApplier.getNameMapping();
                if (!compareNameMapping(oldNameMapping, newNameMapping)) {
                    recorder.append("\n compareNameMapping is not equals!");
                }
                
                long compareNameMapping = System.currentTimeMillis();
                recorder.append("\ncompareNameMapping end").append("cost ms:").append(compareNameMapping-compareNameFilter);

                String oldProperties = oldApplier.getProperties();
                String newProperties = newApplier.getProperties();
                if (!compareProperties(oldApplier.getTargetMhaName(), oldDbCluster.getMhaName(), oldProperties, newProperties)) {
                    recorder.append("\n compareProperties is not equals!");
                }

                long compareProperties = System.currentTimeMillis();
                recorder.append("\ncompareProperties end").append("cost ms:").append(compareProperties-compareNameMapping);
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
                recorder.append("\ncompareNameFilter end").append("cost ms:").append(compareNameFilter-messengerStart);

                String oldProperties = oldMessenger.getProperties();
                String newProperties = newMessenger.getProperties();
                if (!compareMessengerConfigs(oldDbCluster.getMhaName(), oldProperties, newProperties)) {
                    recorder.append("\n compareMessengerConfigs is not equals!");
                }

                long compareProperties = System.currentTimeMillis();
                recorder.append("\ncompareMessengerConfigs end").append("cost ms:").append(compareProperties-compareNameFilter);
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

        List<String> oldNameMappings = Lists.newArrayList(oldNameMapping.split(SEMICOLON));
        List<String> newNameMappings = Lists.newArrayList(newNameMapping.split(SEMICOLON));

        List<String> oldNameMappingInfo = Lists.newArrayList();
        List<String> newNameMappingInfo = Lists.newArrayList();
        for (String  nameMapping : oldNameMappings) {
            String[] mapping = nameMapping.split(COMMA); //bbzmembersaccountshard[01-16]db.uid_accounts[1-8],bbzmembersaccountshard[01-16]db.sjp_uid_accounts[1-8]
            String srcInfo = mapping[0]; // bbzmembersaccountshard[01-16]db.uid_accounts[1-8]
            String destInfo = mapping[1]; // bbzmembersaccountshard[01-16]db.sjp_uid_accounts[1-8]
            ShardInfo srcShardInfo = parseLogicalDbShardInfo(srcInfo);
            ShardInfo destShardInfo = parseLogicalDbShardInfo(destInfo);
            oldNameMappingInfo.add(
                    srcShardInfo.dbShard + srcShardInfo.table + destShardInfo.dbShard + destShardInfo.table
                            + (srcShardInfo.shardCount + destShardInfo.shardCount));
        }

        Map<String,Integer> nameMappingInfoCountMap = Maps.newHashMap();
        for (String  nameMapping : newNameMappings) {
            String[] mapping = nameMapping.split(COMMA); //bbzmembersaccountshard01db.uid_accounts[1-8],bbzmembersaccountshard01db.sjp_uid_accounts[1-8]
            String srcInfo = mapping[0]; // bbzmembersaccountshard01db.uid_accounts[1-8]
            String destInfo = mapping[1]; // bbzmembersaccountshard01db.sjp_uid_accounts[1-8]
            ShardInfo srcShardInfo = parsePhysicalDbShardInfo(srcInfo);
            ShardInfo destShardInfo = parsePhysicalDbShardInfo(destInfo);
            String nameMappingInfo = srcShardInfo.dbShard + srcShardInfo.table + destShardInfo.dbShard + destShardInfo.table;
            Integer count = nameMappingInfoCountMap.getOrDefault(nameMappingInfo, 0);
            nameMappingInfoCountMap.put(nameMappingInfo ,count+2);
        }

        for (Entry<String, Integer> entry : nameMappingInfoCountMap.entrySet()) {
            String info = entry.getKey();
            Integer count = entry.getValue();
            newNameMappingInfo.add(info + count);
        }
        Collections.sort(oldNameMappingInfo);
        Collections.sort(newNameMappingInfo);
        return oldNameMappingInfo.equals(newNameMappingInfo);

    }

    private ShardInfo parseLogicalDbShardInfo(String shardString) {
        String[] dbAndTable = shardString.split(ESCAPE_DOT_REGEX); // bbzmembersaccountshard[01-16]db.uid_accounts[1-8]
        String logicalDb = dbAndTable[0]; // bbzmembersaccountshard[01-16]db
        String logicalTable = dbAndTable[1]; // uid_accounts[1-8]
        String dbShardName = logicalDb.substring(0, logicalDb.indexOf('[')); //bbzmembersaccountshard
        int start = logicalDb.indexOf('['); 
        int end = logicalDb.indexOf(']');
        String shard = logicalDb.substring(start+1,end); //01-16
        String[] shardInfo = shard.split("-");
        String startShard = shardInfo[0];
        String endShard = shardInfo[1];
        if (startShard.startsWith("0")) {
            startShard = startShard.replaceFirst("0","");
        }
        if (endShard.startsWith("0")) {
            endShard = endShard.replaceFirst("0","");
        }
        int shardCount = Integer.parseInt(endShard) - Integer.parseInt(startShard) + 1;
        return new ShardInfo(dbShardName,logicalTable,shardCount);
    }

    private ShardInfo parsePhysicalDbShardInfo(String shardString) {
        String[] dbAndTable = shardString.split(ESCAPE_DOT_REGEX); // bbzmembersaccountshard01db.sjp_uid_accounts[1-8]
        String db = dbAndTable[0]; // bbzmembersaccountshard01db
        String logicalTable = dbAndTable[1]; // sjp_uid_accounts[1-8]
        db = db.replaceFirst("\\d","[");
        String dbShardName = db.substring(0, db.indexOf("["));
        return new ShardInfo(dbShardName,logicalTable,1);
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


    private static class ShardInfo {
        public String dbShard;
        public String table;
        public int shardCount;

        public ShardInfo(String dbShard, String table, int shardCount) {
            this.dbShard = dbShard;
            this.table = table;
            this.shardCount = shardCount;
        }
    }
}
