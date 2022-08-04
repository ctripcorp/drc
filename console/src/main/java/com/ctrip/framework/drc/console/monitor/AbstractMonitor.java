package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.enums.LogTypeEnum;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.config.AbstractConfigBean;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-27
 */
public abstract class AbstractMonitor extends AbstractConfigBean implements Monitor, CLogger {

    public Logger logger = LoggerFactory.getLogger(getClass());

    public static final int INITIAL_DELAY = 30;

    public static final int PERIOD = 30;

    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    protected Integer initialDelay;

    protected Integer period;

    protected TimeUnit timeUnit;

    public void setInitialDelay(Integer initialDelay) {
        this.initialDelay = initialDelay;
    }

    public void setPeriod(Integer period) {
        this.period = period;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(this.getClass().getName());

    Map<Endpoint, WriteSqlOperatorWrapper> sqlOperatorMapper = new ConcurrentHashMap<>();

    @Override
    public void initialize() {
        if(null == initialDelay) {
            setInitialDelay(getDefaultInitialDelay());
        }
        if(null == period) {
            setPeriod(getDefaultPeriod());
        }
        if(null == timeUnit) {
            setTimeUnit(getDefaultTimeUnit());
        }
    }

    @PostConstruct
    @Override
    public void start() {
        initialize();
        scheduledExecutorService.scheduleWithFixedDelay(this::monitorScheduleTask, initialDelay, period, timeUnit);
    }

    public void monitorScheduleTask() {
        try {
            scheduledTask();
        } catch (Throwable t) {
            logger.error("execute monitor schedule task error", t);
        }
    }

    @Override
    public void scheduledTask() throws Throwable{}

    @PreDestroy
    @Override
    public void destroy() {
        scheduledExecutorService.shutdownNow();
    }

    protected WriteSqlOperatorWrapper getSqlOperatorWrapper(Endpoint endpoint) {
        if(sqlOperatorMapper.containsKey(endpoint)) {
            return sqlOperatorMapper.get(endpoint);
        } else {
            WriteSqlOperatorWrapper sqlOperatorWrapper = new WriteSqlOperatorWrapper(endpoint);
            try {
                sqlOperatorWrapper.initialize();
                sqlOperatorWrapper.start();
            } catch (Exception e) {
                logger.error("[{}] sqlOperator({}:{}) initialize error, ", getClass().getName(), endpoint.getHost(), endpoint.getPort(), e);
                return null;
            }
            sqlOperatorMapper.put(endpoint, sqlOperatorWrapper);
            return sqlOperatorWrapper;
        }
    }

    protected void removeSqlOperator(Endpoint endpoint) {
        WriteSqlOperatorWrapper writeSqlOperatorWrapper = sqlOperatorMapper.remove(endpoint);
        if (writeSqlOperatorWrapper != null) {
            try {
                writeSqlOperatorWrapper.stop();
                writeSqlOperatorWrapper.dispose();
            } catch (Exception e) {
                logger.error("[{}] sqlOperatorWrapper({}:{}) stop and dispose: ", getClass().getName(), endpoint.getHost(), endpoint.getPort(), e);
            }
        }
    }

    @Override
    public void cLog(Map<String, String> tags, String msg, LogTypeEnum type, Throwable t) {
        String cLogPrefix = getCLogPrefix(tags);
        String s = cLogPrefix + msg;
        type.log(logger, s, t);
    }

    @Override
    public String getCLogPrefix(Map<String, String> tags) {
        if(tags == null) return StringUtils.EMPTY;
        Iterator<Map.Entry<String, String>> entries = tags.entrySet().iterator();
        Map.Entry<String, String> entry;
        if(!entries.hasNext()) {
            return StringUtils.EMPTY;
        } else {
            entry = entries.next();
        }

        StringBuilder prefix = new StringBuilder();
        prefix.append("[[");
        while(true) {
            prefix.append(entry.getKey()).append('=').append(entry.getValue());
            if(entries.hasNext()) {
                prefix.append(',');
                entry = entries.next();
            } else {
                break;
            }
        }
        prefix.append("]]");
        return prefix.toString();
    }

    @Override
    public int getDefaultInitialDelay() {
        return INITIAL_DELAY;
    }

    @Override
    public int getDefaultPeriod(){
        return PERIOD;
    }

    @Override
    public TimeUnit getDefaultTimeUnit() {
        return TIME_UNIT;
    }
}
