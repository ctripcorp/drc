package com.ctrip.framework.drc.manager.healthcheck.inquirer;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.exception.DrcServerException;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.retry.RestOperationsRetryPolicyFactory;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestOperations;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.QUERY_INFO_LOGGER;


/**
 * @author yongnian
 * @create: 2024/5/20 20:58
 */
public abstract class AbstractInfoInquirer<T> implements InfoInquirer<T> {

    public static final Logger logger = LoggerFactory.getLogger(AbstractInfoInquirer.class);

    public static int RETRY_INTERVAL = 2000;

    protected RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(4, 40, CONNECTION_TIMEOUT, 5000, 0, new RestOperationsRetryPolicyFactory(RETRY_INTERVAL)); //retry by Throwable

    ExecutorService infoInquirerExecutorService;
    private static final String INFO_URL = "http://%s/%s";

    protected AbstractInfoInquirer() {
        int threadNum = DynamicConfig.getInstance().getCmNotifyThread();
        logger.info("{} notify thread num is: {}", getClass().getSimpleName(), threadNum);
        infoInquirerExecutorService = ThreadUtils.newFixedThreadPool(threadNum, "CM-Info-Service");
    }

    @Override
    public Future<List<T>> query(String ipAndPort) {
        String url = getUrl(ipAndPort);
        return infoInquirerExecutorService.submit(() -> {
            try {
                ApiResult<?> apiResult = sendHttp(url);
                if (!checkStatus(apiResult.getStatus())) {
                    QUERY_INFO_LOGGER.warn("[Info] query fail {}, {}", url, apiResult.getMessage());
                }
                return parseData(apiResult);
            } catch (Throwable e) {
                String errMsg = String.format("[Invoke] %s throw exception", url);
                QUERY_INFO_LOGGER.error("{}", errMsg, e);
                throw new DrcServerException(errMsg, e);
            }
        });
    }

    public String getUrl(String ipAndPort) {
        return String.format(INFO_URL, ipAndPort, method());
    }

    abstract String method();


    private ApiResult sendHttp(String url) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON));
        ResponseEntity<ApiResult> response = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(headers), ApiResult.class);
        return response.getBody();
    }

    private boolean checkStatus(int status) {
        ResultCode resultCode = ResultCode.getResultCode(status);
        return Objects.requireNonNull(resultCode) == ResultCode.HANDLE_SUCCESS;
    }

    abstract List<T> parseData(ApiResult<?> data);

    @VisibleForTesting
    public void setRestTemplate(RestOperations restTemplate) {
        this.restTemplate = restTemplate;
    }
}
