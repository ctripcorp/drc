package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Deprecated
public class MetaInfoServiceTwoImpl {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    private DalUtils dalUtils = DalUtils.getInstance();

    public List<String> getProxyUris(String dc) throws Throwable {
        Set<String> dcsInSameRegion = consoleConfig.getDcsInSameRegion(dc);
        List<String> proxyUris = Lists.newArrayList();
        for (String InSameRegion : dcsInSameRegion) {
            Long dcId = dalUtils.getId(TableEnum.DC_TABLE, InSameRegion);
            dalUtils.getProxyTblDao().queryByDcId(dcId,BooleanEnum.FALSE.getCode()).forEach(proxyTbl -> proxyUris.add(proxyTbl.getUri()));
        } 
        return proxyUris;
    }

    public List<String> getAllProxyUris() throws Throwable {
        return dalUtils.getProxyTblDao().queryAll().stream().filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode())).map(ProxyTbl::getUri).collect(Collectors.toList());
    }

}
