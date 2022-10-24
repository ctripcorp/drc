package com.ctrip.framework.drc.applier.mq;

import com.ctrip.framework.drc.applier.container.SpringContextHolder;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.resource.context.MqPosition;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.SpringZkClient;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
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

    @InstanceConfig(path="gtidExecuted")
    public String initialGtidExecuted = "";

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    private GtidSet executedGtidSet ;

    private ZkClient zkClient;

    private File positionFile;

    private String zkPositionPath;

    private String filePositionPath;

    private ExecutorService gtidService = ThreadUtils.newSingleThreadExecutor("Gtid-Consume");

    public ScheduledExecutorService timer = ThreadUtils.newSingleThreadScheduledExecutor("Update-Position-Task");

    @Override
    protected void doInitialize() throws Exception {
        zkPositionPath = MESSENGER_POSITIONS_PATH + "/" + registryKey;
        filePositionPath = MESSENGER_REGISTER_PATH + "/" + registryKey;
        executedGtidSet = new GtidSet(initialGtidExecuted);
        initZkClient();
        positionFile = new File(filePositionPath);
        updatePositionInZk();
        updatePositionInFile();
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
        timer.scheduleWithFixedDelay(() -> {
            try {
                updatePositionInZk();
                updatePositionInFile();
            } catch (Exception e) {
                try {
                    updatePositionInFile();
                } catch (IOException ex) {
                    logger.error("update position error");
                }
            }
        }, 3, 10, TimeUnit.SECONDS);
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
            logger.error("getUuids error for {}", registryKey, e);
        }
        return "";
    }

    private void updatePositionInZk() throws Exception {
        CuratorFramework curatorFramework = zkClient.get();
        checkPath(curatorFramework, zkPositionPath);
        String currentPosition = executedGtidSet.clone().toString();
        curatorFramework.inTransaction().check().forPath(zkPositionPath).and().setData().forPath(zkPositionPath, Codec.DEFAULT.encodeAsBytes(currentPosition)).and().commit();
    }

    private String getPositionFromFile() throws IOException {
        return FileUtils.readFileToString(positionFile);
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
            ApplicationContext applicationContext = SpringContextHolder.getApplicationContext();
            if (applicationContext != null) {
                zkClient = applicationContext.getBean(SpringZkClient.class);
            }
        } catch (Exception e) {
            logger.error("initMetaManager error", e);
        }
    }
}
