package com.ctrip.framework.drc.core.utils;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.google.common.collect.Sets;

import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MONITOR_SCHEMA_NAME;

/**
 * Created by jixinwang on 2023/11/30
 */
public class NameUtils {

    public static String getApplierRegisterKey(String clusterId, Applier applier) {
        String registryKey = clusterId + "." + applier.getTargetMhaName();
        if (ApplyMode.db_transaction_table == ApplyMode.getApplyMode(applier.getApplyMode())) {
            String dbName = NameUtils.toSchema(applier.getNameFilter());
            registryKey = registryKey + "." + dbName;
        }
        return registryKey;
    }

    public static String dotSchemaIfNeed(String name, Applier applier) {
        String finalName = name;
        if (ApplyMode.db_transaction_table == ApplyMode.getApplyMode(applier.getApplyMode())) {
            String dbName = NameUtils.toSchema(applier.getNameFilter());
            finalName = name + "." + dbName;
        }
        return finalName;
    }

    public static String dotSchemaIfNeed(String name, int applyMode, String nameFilter) {
        String finalName = name;
        if (ApplyMode.db_transaction_table == ApplyMode.getApplyMode(applyMode)) {
            String dbName = NameUtils.toSchema(nameFilter);
            finalName = name + "." + dbName;
        }
        return finalName;
    }


    public static String toSchema(String nameFilter) {
        Set<String> schemas = Sets.newHashSet();
        String[] schemaDotTableNames = nameFilter.split(",");

        for (String schemaDotTableName : schemaDotTableNames) {
            String[] schemaAndTable = schemaDotTableName.split("\\\\.");
            if (schemaAndTable.length > 1) {
                schemas.add(schemaAndTable[0].toLowerCase());
                continue;
            }

            String[] schemaAndTable2 = schemaDotTableName.split("\\.");
            if (schemaAndTable2.length > 1) {
                schemas.add(schemaAndTable2[0].toLowerCase());
            }
        }
        schemas.remove(DRC_MONITOR_SCHEMA_NAME);
        if (schemas.size() == 1) {
            return schemas.iterator().next();
        } else {
            throw new RuntimeException("get schema for name-filter error, with schemas: " + schemas);
        }
    }
}
