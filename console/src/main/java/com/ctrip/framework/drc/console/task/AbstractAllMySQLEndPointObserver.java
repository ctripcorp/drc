package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import com.google.common.collect.Maps;
import org.unidal.tuple.Triple;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MYSQL_LOGGER;

/**
 * @ClassName AbstractAllMySQLEndPointObserver
 * @Author haodongPan
 * @Date 2022/7/25 17:53
 * @Version: $
 */
public abstract class AbstractAllMySQLEndPointObserver extends AbstractLeaderAwareMonitor implements MasterMySQLEndpointObserver , SlaveMySQLEndpointObserver {

    protected Map<MetaKey, MySqlEndpoint> masterMySQLEndpointMap = Maps.newConcurrentMap();

    protected Map<MetaKey, MySqlEndpoint> slaveMySQLEndpointMap = Maps.newConcurrentMap();

    protected String regionName;

    protected List<String> dcsInRegion;

    protected String localDcName;

    protected boolean onlyCarePart;

    @Override
    public void initialize() {
        super.initialize();
        setObservationRange();
    }
    
    public void update(Object args, Observable observable) {
        if (observable instanceof MasterMySQLEndpointObservable) {
            updateMySQLEndpointMap((Triple<MetaKey, MySqlEndpoint, ActionEnum>) args, masterMySQLEndpointMap);
        } else if (observable instanceof SlaveMySQLEndpointObservable) {
            updateMySQLEndpointMap((Triple<MetaKey, MySqlEndpoint, ActionEnum>) args, slaveMySQLEndpointMap);
        }
    }

    private void updateMySQLEndpointMap(Triple<MetaKey, MySqlEndpoint, ActionEnum> msg, Map<MetaKey, MySqlEndpoint> mySQLEndpointMap) {

        MetaKey metaKey = msg.getFirst();
        MySqlEndpoint mySQLEndpoint = msg.getMiddle();
        ActionEnum action = msg.getLast();

        if (onlyCarePart && !isCare(metaKey)) {
            CONSOLE_MYSQL_LOGGER.warn("[OBSERVE][{}] localDc-{} Region not interested in {}({})", getClass().getName(), localDcName, metaKey, mySQLEndpoint.getSocketAddress());
            return;
        }

        if (ActionEnum.ADD.equals(action) || ActionEnum.UPDATE.equals(action)) {
            CONSOLE_MYSQL_LOGGER.info("[OBSERVE][{}] {} {}({})", getClass().getName(), action.name(), metaKey, mySQLEndpoint.getSocketAddress());
            MySqlEndpoint oldEndpoint = mySQLEndpointMap.get(metaKey);
            if (oldEndpoint != null) {
                CONSOLE_MYSQL_LOGGER.info("[OBSERVE][{}] {} clear old {}({})", getClass().getName(), action.name(), metaKey, oldEndpoint.getSocketAddress());
                clear(oldEndpoint,metaKey);
            }
            mySQLEndpointMap.put(metaKey, mySQLEndpoint);
        } else if (ActionEnum.DELETE.equals(action)) {
            CONSOLE_MYSQL_LOGGER.info("[OBSERVE][{}] {} {}", getClass().getName(), action.name(), metaKey);
            MySqlEndpoint oldEndpoint = mySQLEndpointMap.remove(metaKey);
            if (oldEndpoint != null) {
                CONSOLE_MYSQL_LOGGER.info("[OBSERVE][{}] {} clear old {}({})", getClass().getName(), action.name(), metaKey, oldEndpoint.getSocketAddress());
                clear(oldEndpoint,metaKey);
            }
        }
    }

    protected void clear(Endpoint endpoint,MetaKey metaKey) {
        removeSqlOperator(endpoint);
        clearResource(endpoint,metaKey);
    }
    
    public abstract void clearResource(Endpoint endpoint,MetaKey metaKey);

    public abstract void setLocalDcName();

    public abstract void setLocalRegionInfo();

    public abstract void setOnlyCarePart();

    public abstract boolean isCare(MetaKey metaKey);

    private void setObservationRange() {
        setOnlyCarePart();
        setLocalDcName();
        setLocalRegionInfo();
    }
}
