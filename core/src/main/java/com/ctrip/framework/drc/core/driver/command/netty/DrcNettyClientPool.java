package com.ctrip.framework.drc.core.driver.command.netty;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.pool.ReturnObjectException;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author limingdong
 * @create 2021/4/9
 */
public class DrcNettyClientPool extends AbstractLifecycle implements SimpleObjectPool<NettyClient> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private ObjectPool<NettyClient> objectPool;

    private NettyClientFactory pooledObjectFactory;

    private GenericObjectPoolConfig config;

    private Endpoint target;

    public DrcNettyClientPool(Endpoint target, NettyClientFactory nettyClientFactory) {
        this.target = target;
        this.config = createDefaultConfig();
        this.pooledObjectFactory = nettyClientFactory;
    }

    @Override
    protected void doInitialize() throws Exception {
        this.pooledObjectFactory.start();
        GenericObjectPool<NettyClient> genericObjectPool = new GenericObjectPool<>(pooledObjectFactory, config);
        genericObjectPool.setTestOnBorrow(true);
        genericObjectPool.setTestOnCreate(true);
        this.objectPool = genericObjectPool;
    }

    @Override
    public NettyClient borrowObject() throws BorrowObjectException {
        try {
            logger.debug("[borrowObject][begin]{}", target);
            NettyClient value = this.objectPool.borrowObject();
            logger.debug("[borrowObject][end]{}, {}", target, value);
            return value;
        } catch (Exception e) {
            throw new BorrowObjectException("borrow " + target, e);
        }
    }

    @Override
    public void returnObject(NettyClient value) throws ReturnObjectException {
        try {
            logger.debug("[returnObject]{}, {}", target, value);
            this.objectPool.returnObject(value);
        } catch (Exception e) {
            throw new ReturnObjectException("return " + target + " " + value, e);
        }
    }

    /**
     * invoke when stop
     */
    @Override
    public void clear() {
        try {
            this.objectPool.clear();
        } catch (Exception e) {
            logger.error("clear pool error");
        }
    }

    @Override
    public String desc() {
        return pooledObjectFactory.toString();
    }

    private static GenericObjectPoolConfig createDefaultConfig() {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setJmxEnabled(false);
        config.setMaxTotal(1);
        config.setMaxIdle(1);
        return config;
    }

    @Override
    protected void doStop() throws Exception {
        objectPool.close();
        this.pooledObjectFactory.stop();
    }
}
