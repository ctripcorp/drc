package com.ctrip.framework.drc.applier.mq;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.utils.SpringUtils;
import com.ctrip.framework.drc.fetcher.resource.context.MqPosition;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.SpringZkClient;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.context.ApplicationContext;

import java.io.*;
import java.util.concurrent.*;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.MESSENGER_POSITIONS_PATH;
import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.MESSENGER_REGISTER_PATH;

/**
 * Created by jixinwang on 2022/10/19
 */
public class MqPositionResource extends AbstractResource implements MqPosition {

    private static final int RETRY_TIME = 3;

    private static final int PERSIST_POSITION_PERIOD_TIME = 10;

    @InstanceConfig(path="gtidExecuted")
    public String initialGtidExecuted = "";

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    private GtidSet executedGtidSet ;

    private ZkClient zkClient;

    private File positionFile;

    private String zkPositionPath;

    private ExecutorService gtidService = ThreadUtils.newSingleThreadExecutor("MQ-Update-Position");

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("MQ-Persist-Position-Task");

    @Override
    protected void doInitialize() throws Exception {
        zkPositionPath = MESSENGER_POSITIONS_PATH + "/" + registryKey;
        executedGtidSet = new GtidSet(initialGtidExecuted);
        String filePositionPath = MESSENGER_REGISTER_PATH + "/" + registryKey;
        positionFile = new File(filePositionPath);
        initZkClient();
        persistPosition();
        startUpdatePositionSchedule();
    }

    @Override
    public void updatePosition(String gtid) {
        gtidService.submit(() -> {
            executedGtidSet.add(gtid);
        });
    }

    @Override
    public String getPosition() throws IOException {
        GtidSet gtidSetFromZk = new GtidSet(getPositionFromZk());
        GtidSet gtidSetFromFile = new GtidSet(getPositionFromFile());
        return gtidSetFromZk.union(gtidSetFromFile).toString();
    }

    private void startUpdatePositionSchedule() {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                persistPosition();
                logger.info("[MQ][{}] persist position schedule success", registryKey);
            } catch (Throwable t) {
                logger.error("[MQ][{}] persist position schedule error", registryKey, t);
            }
        }, PERSIST_POSITION_PERIOD_TIME, PERSIST_POSITION_PERIOD_TIME, TimeUnit.SECONDS);
    }

    private void persistPosition() {
        Boolean res = new RetryTask<>(new ZkPositionUpdateTask(), RETRY_TIME).call();
        if (res == null) {
            logger.error("[MQ][{}] persist position in zk error", registryKey);
            try {
                updatePositionInFile();
            } catch (IOException e) {
                logger.error("[MQ][{}] persist position error", registryKey);
            }
        }
    }

    private String getPositionFromZk() {
        CuratorFramework curatorFramework = zkClient.get();
        try {
            if (!checkPath(curatorFramework, zkPositionPath)) {
                return "";
            }
            byte[] uuidArray = curatorFramework.getData().forPath(zkPositionPath);
            return Codec.DEFAULT.decode(uuidArray, String.class);
        } catch (Exception e) {
            logger.error("[MQ][{}] get position from zk error", registryKey, e);
        }
        return "";
    }

    private String getPositionFromFile() throws IOException {
        if (positionFile.exists()) {
            return FileUtils.readFileToString(positionFile);
        }
        return StringUtils.EMPTY;
    }

    private void updatePositionInFile() throws IOException {
        String currentPosition = executedGtidSet.clone().toString();
        if (!positionFile.exists()) {
            Files.createParentDirs(positionFile);
            Files.touch(positionFile);
        }
        FileUtils.writeStringToFile(positionFile, currentPosition);
    }

    private boolean checkPath(CuratorFramework curatorFramework, String registerPath) throws Exception {
        if (curatorFramework.checkExists().forPath(registerPath) == null) {
            curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(registerPath);
            return false;
        }
        return true;
    }

    private void initZkClient() {
        try {
            ApplicationContext applicationContext = SpringUtils.getApplicationContext();
            if (applicationContext != null) {
                zkClient = applicationContext.getBean(SpringZkClient.class);
            }
        } catch (Exception e) {
            logger.error("[MQ][{}]  init zk client error", registryKey, e);
        }
    }

    class ZkPositionUpdateTask implements NamedCallable<Boolean> {
        @Override
        public Boolean call() throws Exception {
            CuratorFramework curatorFramework = zkClient.get();
            checkPath(curatorFramework, zkPositionPath);
            String currentPosition = executedGtidSet.clone().toString();
            curatorFramework.inTransaction().check().forPath(zkPositionPath).and().setData().forPath(zkPositionPath, Codec.DEFAULT.encodeAsBytes(currentPosition)).and().commit();
            return true;
        }
    }

    @Override
    protected void doDispose() throws Exception {
        persistPosition();
        logger.info("[MQ][{}] persist mq position when mq position resource dispose", registryKey);

        if (gtidService != null) {
            gtidService.shutdown();
            gtidService = null;
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
            scheduledExecutorService = null;
        }
    }
}
