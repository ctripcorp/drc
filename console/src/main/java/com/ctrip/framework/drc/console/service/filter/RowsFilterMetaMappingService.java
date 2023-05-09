package com.ctrip.framework.drc.console.service.filter;

import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMappingCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMessageCreateParam;
import com.ctrip.framework.drc.console.vo.filter.RowsFilterMetaMappingVO;
import com.ctrip.framework.drc.console.vo.filter.RowsFilterMetaMessageVO;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/26 11:04
 */
public interface RowsFilterMetaMappingService {

    boolean createMetaMessage(RowsFilterMetaMessageCreateParam param) throws SQLException;

    boolean createOrUpdateMetaMapping(RowsFilterMetaMappingCreateParam param) throws SQLException;

    List<RowsFilterMetaMessageVO> getMetaMessages(String metaFilterName) throws SQLException;

    RowsFilterMetaMappingVO getMetaMappings(Long metaFilterId) throws SQLException;

    boolean deleteMetaMessage(Long metaFilterId) throws SQLException;
}
