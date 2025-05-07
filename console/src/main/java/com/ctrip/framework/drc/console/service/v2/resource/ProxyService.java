package com.ctrip.framework.drc.console.service.v2.resource;

import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.core.http.ApiResult;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/12/6 20:26
 */
public interface ProxyService {

    List<String> getProxyUris(String dc) throws Exception;

    List<String> getProxyUris(String dc, boolean src) throws Exception;


    List<String> getRelayProxyUris() throws Exception;

    ApiResult deleteProxy(ProxyDto proxyDto);

    ApiResult inputDc(String dc);

    ApiResult inputBu(String bu);

    void inputProxy(String dc, String ip) throws Exception;

}
