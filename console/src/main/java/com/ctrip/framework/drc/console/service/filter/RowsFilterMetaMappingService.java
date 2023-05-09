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

    boolean createMetaMessage(RowsFilterMetaMessageCreateParam param) throws Exception;

    boolean createOrUpdateMetaMapping(RowsFilterMetaMappingCreateParam param) throws Exception;

    List<RowsFilterMetaMessageVO> getMetaMessages(String metaFilterName) throws Exception;

    RowsFilterMetaMappingVO getMetaMappings(Long metaFilterId) throws Exception;

    boolean deleteMetaMessage(Long metaFilterId) throws Exception;

    List<String> getTargetSubEnvs();
}
