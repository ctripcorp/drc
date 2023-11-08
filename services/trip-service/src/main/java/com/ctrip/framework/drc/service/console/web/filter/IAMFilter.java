package com.ctrip.framework.drc.service.console.web.filter;

import com.ctrip.basebiz.offline.iam.apifacade.contract.IAMFacadeServiceClient;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.ResultItem;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.VerifyByBatchCodeRequestType;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.VerifyByBatchCodeResponseType;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.infosec.sso.client.CtripSSOTools;
import com.ctriposs.baiji.rpc.common.types.AckCodeType;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Map.Entry;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName IAMFilter
 * @Author haodongPan
 * @Date 2023/10/23 17:10
 * @Version: $
 */

public class IAMFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(IAMFilter.class);
    private IAMFacadeServiceClient iamSoaService = IAMFacadeServiceClient.getInstance();
    private IAMFilterService iamFilterService = new IAMFilterService();
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        logger.info("IAMFilter init");
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if(iamFilterService.iamFilterEnable()) {
            if(hasPermission(request, response)) {
                chain.doFilter(request, response);
            }
        } else {
            chain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {

    }
    
    private boolean hasPermission(ServletRequest request, ServletResponse response) throws IOException {
        String eid = CtripSSOTools.getEid();
        if (StringUtils.isBlank(eid)) { // redirect sso login,or by pass request
            return true;
        }
        
        HttpServletRequest req = (HttpServletRequest) request;
        String requestURL = req.getRequestURL().toString();
        String apiPermissionCode = iamFilterService.getApiPermissionCode(requestURL);
        if (StringUtils.isBlank(apiPermissionCode)) { // no need check permission
            return true;
        }

        VerifyByBatchCodeRequestType verifyRequest = new VerifyByBatchCodeRequestType(Lists.newArrayList(apiPermissionCode), eid);
        try {
            VerifyByBatchCodeResponseType verifyResponse = iamSoaService.verifyByBatchCode(verifyRequest);
            if (!verifyResponse.getResponseStatus().ack.equals(AckCodeType.Success)) {
                logger.error("verifyByBatchCode error,request:{}",verifyRequest);
                writeResult(response, "verifyByBatchCode error");
                return false;
            }
            Map<String, ResultItem> results = verifyResponse.getResults();
            for (Entry<String, ResultItem> res : results.entrySet()) {
                String permissionCode = res.getKey();
                ResultItem resultItem = res.getValue();
                if (!resultItem.hasPermission) {
                    logger.info("IAMFilter nopermission eid:{}, permissionCode:{}", eid, permissionCode);
                    writeResult(response, permissionCode);
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("verifyByBatchCode error,request:{}",verifyRequest,e);
            writeResult(response, "verifyByBatchCode error");
            return false;
        }
    }
    
    private void writeResult(ServletResponse response, String permissionCode) throws IOException {
        PrintWriter writer = response.getWriter();
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        writer.print(JsonUtils.toJson(ApiResult.getNoPermissionResult(permissionCode)));
        writer.flush();
    }
}
