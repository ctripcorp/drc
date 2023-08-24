package com.ctrip.framework.drc.console.service.remote.qconfig;


import com.ctrip.framework.drc.console.utils.MySqlUtils.TableSchemaName;
import java.util.List;

public interface QConfigService {

    boolean addOrUpdateDalClusterMqConfig(String fileDc, String topic, String fullTableName, String tag,
            List<TableSchemaName> matchTables);


    boolean removeDalClusterMqConfigIfNecessary(String fileDc, String topic, String table, String tag,
            List<TableSchemaName> matchTables, List<String> otherTablesByTopic);

    boolean updateDalClusterMqConfig(String dcName, String topic, String dalClusterName, List<TableSchemaName> matchTables);

}
