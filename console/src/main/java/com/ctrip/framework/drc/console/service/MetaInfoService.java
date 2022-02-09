package com.ctrip.framework.drc.console.service;

import java.sql.SQLException;

public interface MetaInfoService {
    String getTargetName(String mha, String remoteMha) throws SQLException;
}
