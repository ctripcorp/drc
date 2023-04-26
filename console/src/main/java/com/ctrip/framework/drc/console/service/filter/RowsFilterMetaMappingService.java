package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMappingCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMessageCreateParam;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/4/26 11:04
 */
public interface RowsFilterMetaMappingService {

    boolean createMetaMessage(RowsFilterMetaMessageCreateParam param) throws SQLException;

    boolean createMetaMapping(RowsFilterMetaMappingCreateParam param) throws SQLException;
}
