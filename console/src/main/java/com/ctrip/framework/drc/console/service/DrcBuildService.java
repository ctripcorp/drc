package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.utils.MySqlUtils;

import java.util.List;


public interface DrcBuildService {

    // route By mha
    List<String> queryTablesWithNameFilter(String mha, String nameFilter);

    // route By mhaName
    List<MySqlUtils.TableSchemaName> getMatchTable(String namespace, String name, String mhaName, Integer type);
}
