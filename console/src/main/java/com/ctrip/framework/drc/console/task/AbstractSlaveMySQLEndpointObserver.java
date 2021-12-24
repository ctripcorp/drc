package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import com.google.common.collect.Maps;
import org.unidal.tuple.Triple;

import java.util.Map;

/**
 * @Author: hbshen
 * @Date: 2021/4/27
 */
public abstract class AbstractSlaveMySQLEndpointObserver extends AbstractMonitor implements SlaveMySQLEndpointObserver {

    protected Map<MetaKey, MySqlEndpoint> slaveMySQLEndpointMap = Maps.newConcurrentMap();

    protected String localDcName;

    protected boolean onlyCareLocal;

    @Override
    public void initialize() {
        super.initialize();
        setLocalDcName();
        setOnlyCareLocal();
    }

    @Override
    public void update(Object args, Observable observable) {
        if (observable instanceof SlaveMySQLEndpointObservable) {
            Triple<MetaKey, MySqlEndpoint, ActionEnum> message = (Triple<MetaKey, MySqlEndpoint, ActionEnum>) args;
            MetaKey metaKey = message.getFirst();
            MySqlEndpoint slaveMySQLEndpoint = message.getMiddle();
            ActionEnum action = message.getLast();

            if(onlyCareLocal && !metaKey.getDc().equalsIgnoreCase(localDcName)) {
                logger.warn("[OBSERVE][{}] {} not interested in {}({})", getClass().getName(), localDcName, metaKey, slaveMySQLEndpoint.getSocketAddress());
                return;
            }

            if(ActionEnum.ADD.equals(action) || ActionEnum.UPDATE.equals(action)) {
                logger.info("[OBSERVE][{}] {} {}({})", getClass().getName(), action.name(), metaKey, slaveMySQLEndpoint.getSocketAddress());
                MySqlEndpoint oldEndpoint = slaveMySQLEndpointMap.get(metaKey);
                if (oldEndpoint != null) {
                    logger.info("[OBSERVE][{}] {} clear old {}({})", getClass().getName(), action.name(), metaKey, oldEndpoint.getSocketAddress());
                    clearOldEndpointResource(oldEndpoint);
                }
                slaveMySQLEndpointMap.put(metaKey, slaveMySQLEndpoint);
            } else if (ActionEnum.DELETE.equals(action)) {
                logger.info("[OBSERVE][{}] {} {}", getClass().getName(), action.name(), metaKey);
                MySqlEndpoint oldEndpoint = slaveMySQLEndpointMap.remove(metaKey);
                if (oldEndpoint != null) {
                    logger.info("[OBSERVE][{}] {} clear old {}({})", getClass().getName(), action.name(), metaKey, oldEndpoint.getSocketAddress());
                    clearOldEndpointResource(oldEndpoint);
                }
            }
        }
    }

    public abstract void clearOldEndpointResource(Endpoint endpoint);

    public abstract void setLocalDcName();

    public abstract void setOnlyCareLocal();
}
