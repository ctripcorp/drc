package com.ctrip.framework.drc.console.service.impl.inquirer;

import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.exception.DrcServerException;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.QUERY_INFO_LOGGER;

/**
 * Created by shiruixin
 * 2024/9/11 17:32
 */
public abstract class AbstractInquirer<T> implements Inquirer<T> {
    public static final Logger logger = LoggerFactory.getLogger(AbstractInquirer.class);
    private static final String INFO_URL = "http://%s/%s";
    ExecutorService executorService;


    abstract String method();

    protected AbstractInquirer() {
        executorService = ThreadUtils.newFixedThreadPool(5, "Console-Inquirer-Service");
    }

    abstract List<T> parseData(ApiResult<?> data);

    private boolean checkStatus(int status) {
        ResultCode resultCode = ResultCode.getResultCode(status);
        return Objects.requireNonNull(resultCode) == ResultCode.HANDLE_SUCCESS;
    }

    @Override
    public Future<List<T>> query(String ipAndPort) {
        String url = getUrl(ipAndPort);
        return executorService.submit(() -> {
            try {
                ApiResult<?> apiResult = HttpUtils.get(url);
                if (!checkStatus(apiResult.getStatus())) {
                    logger.warn("[Info] query fail {}, {}", url, apiResult.getMessage());
                }
                return parseData(apiResult);
            } catch (Throwable e) {
                String errMsg = String.format("[Invoke] %s throw exception", url);
                logger.error("{}", errMsg, e);
                throw new DrcServerException(errMsg, e);
            }
        });

    }

    public String getUrl(String ipAndPort) {
        return String.format(INFO_URL, ipAndPort, method());
    }
}
