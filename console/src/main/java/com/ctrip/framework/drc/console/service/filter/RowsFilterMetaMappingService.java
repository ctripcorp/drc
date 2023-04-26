package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMappingCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMessageCreateParam;
import com.ctrip.framework.drc.console.vo.filter.RowsFilterMetaMappingVO;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/26 11:04
 */
public interface RowsFilterMetaMappingService {

    boolean createMetaMessage(RowsFilterMetaMessageCreateParam param) throws SQLException;

    boolean createMetaMapping(RowsFilterMetaMappingCreateParam param) throws SQLException;

    List<RowsFilterMetaMappingVO> getMetaMappings(String metaFilterName) throws SQLException;
}
