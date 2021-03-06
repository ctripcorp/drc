package com.ctrip.framework.drc.manager.ha.meta.server.impl;

import com.ctrip.xpipe.retry.RetryPolicyFactories;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestOperations;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public abstract class AbstractService {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected static int DEFAULT_MAX_PER_ROUTE = Integer.parseInt(System.getProperty("max-per-route", "1000"));
    protected static int DEFAULT_MAX_TOTAL = Integer.parseInt(System.getProperty("max-per-route", "10000"));
    protected static int DEFAULT_RETRY_TIMES = Integer.parseInt(System.getProperty("retry-times", "1"));
    protected static int DEFAULT_CONNECT_TIMEOUT = Integer.parseInt(System.getProperty("connect-timeout", "1000"));
    public static int DEFAULT_SO_TIMEOUT = Integer.parseInt(System.getProperty("so-timeout", "6000"));

    public static int DEFAULT_RETRY_INTERVAL_MILLI = Integer
            .parseInt(System.getProperty("metaserver.retryIntervalMilli", "5"));

    private int retryTimes;
    private int retryIntervalMilli;
    protected RestOperations restTemplate;

    public AbstractService() {
        this(DEFAULT_RETRY_TIMES, DEFAULT_RETRY_INTERVAL_MILLI);
    }

    public AbstractService(int retryTimes, int retryIntervalMilli) {

        this.retryTimes = retryTimes;
        this.retryIntervalMilli = retryIntervalMilli;
        this.restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(
                DEFAULT_MAX_PER_ROUTE,
                DEFAULT_MAX_TOTAL,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_SO_TIMEOUT,
                retryTimes,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(retryIntervalMilli));
    }
}
