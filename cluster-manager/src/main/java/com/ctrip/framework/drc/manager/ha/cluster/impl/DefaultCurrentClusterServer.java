package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.framework.drc.manager.ha.cluster.*;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.config.ClusterZkConfig;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class DefaultCurrentClusterServer extends AbstractClusterServer implements CurrentClusterServer, TopElement, Observer {

    @Autowired
    private ZkClient zkClient;

    @Autowired
    protected ClusterManagerConfig config;

    @Autowired
    protected SlotManager slotManager;

    @Autowired
    private ClusterManagerLeaderElector clusterManagerLeaderElector;

    @Autowired
    private ClusterServerStateManager clusterServerStateManager;

    private String currentServerId;

    private String serverPath;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    private PersistentNode persistentNode;

    public DefaultCurrentClusterServer() {

    }

    @Override
    protected void doInitialize() throws Exception {

        this.currentServerId = config.getClusterServerId();
        serverPath = ClusterZkConfig.getClusterManagerRegisterPath() + "/" + currentServerId;

        setServerId(currentServerId);
        setClusterServerInfo(new ClusterServerInfo(config.getClusterServerIp(), config.getClusterServerPort()));
        clusterServerStateManager.initialize();
    }

    @Override
    protected void doStart() throws Exception {

        CuratorFramework client = zkClient.get();

        if(client.checkExists().forPath(serverPath) != null){

            byte []data = client.getData().forPath(serverPath);
            throw new IllegalStateException("server already exist:" + new String(data));
        }

        persistentNode = new PersistentNode(zkClient.get(), CreateMode.EPHEMERAL, false, serverPath, Codec.DEFAULT.encodeAsBytes(getClusterInfo()));
        persistentNode.start();
        clusterServerStateManager.start();
        clusterServerStateManager.addObserver(this);
    }

    @Override
    public int getOrder() {
        return ORDER;
    }

    @Override
    protected void doStop() throws Exception {
        persistentNode.close();
    }

    @Override
    protected synchronized void updateClusterState(ServerStateEnum stateEnum) {
        // persist state, so leader can perceive server state when reconnect
        super.updateClusterState(stateEnum);
        try {
            persistentNode.waitForInitialCreate(1, TimeUnit.SECONDS);
            persistentNode.setData(Codec.DEFAULT.encodeAsBytes(getClusterInfo()));
        } catch (Exception e) {
            ClusterServerInfo currData = Codec.DEFAULT.decode(persistentNode.getData(), ClusterServerInfo.class);
            logger.warn("[updateClusterState] persistentNode setData. stateEnum: {}, currData: {}", stateEnum, currData, e);
        }
    }

    @Override
    public void update(Object args, Observable observable) {
        if (args instanceof ServerStateEnum) {
            updateClusterState((ServerStateEnum) args);
        }
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void setConfig(ClusterManagerConfig config) {
        this.config = config;
    }

    @Override
    public void notifySlotChange(int slotId) {
        new SlotRefreshCommand(slotId).execute(executors);
    }


    @Override
    public CommandFuture<Void> addSlot(int slotId) {
        return new SlotAddCommand(slotId).execute(executors);
    }

    @Override
    public CommandFuture<Void> deleteSlot(int slotId) {
        return new SlotDeleteCommand(slotId).execute(executors);
    }

    @Override
    public CommandFuture<Void> exportSlot(int slotId) {

        return new SlotExportCommand(slotId).execute(executors);
    }

    @Override
    public CommandFuture<Void> importSlot(int slotId) {
        return new SlotImportCommand(slotId).execute(executors);
    }

    @Override
    public Set<Integer> slots() {
        Set<Integer> slots = slotManager.getSlotsByServerId(currentServerId);
        if(slots == null){
            return new HashSet<>();
        }
        return slots;
    }

    @Override
    public boolean isLeader() {
        return clusterManagerLeaderElector.amILeader();
    }

    protected boolean isExporting(Object key){

        SlotInfo slotInfo = slotManager.getSlotInfoByKey(key);
        if(slotInfo.getSlotState() == SLOT_STATE.MOVING){
            return true;
        }
        return false;
    }

    public void doWaitForSlotCommandsFinish() {
        //TODO wait for slot to clean export info
    }

    @Override
    public boolean hasKey(Object key) {

        String serverId = slotManager.getServerIdByKey(key);
        if(serverId == null){
            return false;
        }
        return serverId.equalsIgnoreCase(this.getServerId());
    }


    class SlotImportCommand extends AbstractCommand<Void> {

        private int slotId;
        public SlotImportCommand(int slotId){
            this.slotId = slotId;
        }

        @Override
        public String getName() {
            return "SlotImport";
        }

        @Override
        protected void doExecute() throws Exception {

            slotManager.refresh(slotId);
            SlotInfo slotInfo = slotManager.getSlotInfo(slotId);
            if(slotInfo.getSlotState() == SLOT_STATE.MOVING && slotInfo.getToServerId().equalsIgnoreCase(getServerId())){
                logger.info("[doExecute][import({})]{}, {}", currentServerId, slotId, slotInfo);
            }else{
                throw new IllegalStateException("error import " + slotId + "," + slotInfo);
            }
            doSlotImport(slotId);
            future().setSuccess();
        }
        @Override
        protected void doReset() {

        }
    }

    class SlotExportCommand extends AbstractCommand<Void>{

        private int slotId;
        public SlotExportCommand(int slotId){
            this.slotId = slotId;
        }

        @Override
        public String getName() {
            return "SlotExport";
        }

        @Override
        protected void doExecute() throws Exception {

            slotManager.refresh(slotId);
            SlotInfo slotInfo = slotManager.getSlotInfo(slotId);
            if(slotInfo.getSlotState() == SLOT_STATE.MOVING && slotInfo.getServerId().equalsIgnoreCase(getServerId())){
                logger.info("[doExecute][export({}){}, {},{}", currentServerId, slotId, slotInfo, getServerId());
            }else{
                throw new IllegalStateException("error export " + slotId + "," + slotInfo);
            }
            doSlotExport(slotId);
            future().setSuccess();
        }
        @Override
        protected void doReset() {

        }
    }

    class SlotAddCommand extends AbstractCommand<Void>{

        private int slotId;
        public SlotAddCommand(int slotId) {
            this.slotId = slotId;
        }

        @Override
        public String getName() {
            return "SlotRefreshCommand";
        }

        @Override
        protected void doExecute() throws Exception {

            slotManager.refresh(slotId);
            SlotInfo slotInfo = slotManager.getSlotInfo(slotId);
            if(slotInfo.getSlotState() == SLOT_STATE.NORMAL && slotInfo.getServerId().equalsIgnoreCase(getServerId())){
                logger.info("[doExecute][slot add]{}, {}", slotId, slotInfo);
            }else{
                throw new IllegalStateException("error add " + slotId + "," + slotInfo);
            }
            doSlotAdd(slotId);
            future().setSuccess();
        }

        @Override
        protected void doReset(){
        }
    }

    class SlotDeleteCommand extends AbstractCommand<Void>{

        private int slotId;
        public SlotDeleteCommand(int slotId) {
            this.slotId = slotId;
        }

        @Override
        public String getName() {
            return "SlotRefreshCommand";
        }

        @Override
        protected void doExecute() throws Exception {

            slotManager.refresh(slotId);
            SlotInfo slotInfo = slotManager.getSlotInfo(slotId);
            if(slotInfo.getSlotState() == SLOT_STATE.NORMAL && !slotInfo.getServerId().equalsIgnoreCase(getServerId())){
                logger.info("[doExecute][slot delete]{}, {}", slotId, slotInfo);
            }else{
                throw new IllegalStateException("error delete " + slotId + "," + slotInfo);
            }
            doSlotDelete(slotId);
            future().setSuccess();
        }
        @Override
        protected void doReset(){
        }
    }

    class SlotRefreshCommand extends AbstractCommand<Void>{

        private int slotId;
        public SlotRefreshCommand(int slotId) {
            this.slotId = slotId;
        }

        @Override
        public String getName() {
            return "SlotRefreshCommand";
        }

        @Override
        protected void doExecute() throws Exception {

            slotManager.refresh(slotId);
            future().setSuccess();
        }
        @Override
        protected void doReset(){
        }
    }

    protected void doSlotImport(int slotId) {
    }

    protected void doSlotAdd(int slotId) {
    }

    protected  void doSlotExport(int slotId) {
    }

    protected void doSlotDelete(int slotId) {
    }

    @Override
    public void setExecutors(Executor executors) {
        this.executors = executors;
    }
}
