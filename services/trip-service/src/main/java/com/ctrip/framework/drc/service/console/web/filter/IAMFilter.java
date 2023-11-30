package com.ctrip.framework.drc.service.console.web.filter;


import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.core.service.user.IAMService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.infosec.sso.client.CtripSSOTools;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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
    private IAMService iamServiceImpl = ServicesUtil.getIAMService();
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        logger.info("IAMFilter init");
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if(iamServiceImpl.iamFilterEnable()) {
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
        String apiPermissionCode = iamServiceImpl.matchApiPermissionCode(requestURL);
        if (StringUtils.isBlank(apiPermissionCode)) { // no need check permission
            return true;
        }
        Pair<Boolean, String> result = iamServiceImpl.checkPermission(Lists.newArrayList(apiPermissionCode), eid);
        if (result.getLeft()) {
            return true;
        } else {
            writeResult(response, result.getRight());
            return false;
        }
    }
    
    private void writeResult(ServletResponse response, String message) throws IOException {
        PrintWriter writer = response.getWriter();
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        writer.print(JsonUtils.toJson(ApiResult.getNoPermissionResult(message)));
        writer.flush();
    }
}
