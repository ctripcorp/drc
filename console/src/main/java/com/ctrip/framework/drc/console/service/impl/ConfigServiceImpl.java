package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.service.ConfigService;
import com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.EnumSet;
import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-07
 */
@Service
public class ConfigServiceImpl implements ConfigService {

    @Override
    public String[] getAllDrcSupportDataTypes() {
        List<String> dataTypes = Lists.newArrayList();
        EnumSet.allOf(MysqlFieldType.class)
                .forEach(mysqlFieldType -> {
                    final String[] literals = mysqlFieldType.getLiterals();
                    for(String literal : literals) {
                        dataTypes.add(literal);
                    }
                });
        String[] typeArr = new String[dataTypes.size()];
        dataTypes.toArray(typeArr);
        return typeArr;
    }
}
