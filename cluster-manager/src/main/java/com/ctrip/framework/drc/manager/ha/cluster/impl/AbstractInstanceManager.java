package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.entity.SimpleInstance;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.config.InfoDto;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.QUERY_INFO_LOGGER;

/**
 * response for registering zk and stopping instance
 *
 * @Author limingdong
 * @create 2020/5/5
 */
public abstract class AbstractInstanceManager extends AbstractCurrentMetaObserver {

    @Autowired
    protected InstanceStateController instanceStateController;

    @Autowired
    protected ClusterManagerConfig clusterManagerConfig;

    @Autowired
    private DefaultClusterManagers clusterServers;


    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create(getClass().getSimpleName()));

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        scheduled.scheduleWithFixedDelay(getChecker(), 30000, 30000, TimeUnit.MILLISECONDS);
    }

    protected abstract InstancePeriodicallyChecker getChecker();

    @Override
    protected void doStop() throws Exception {
        super.doStop();
    }

    protected abstract class InstancePeriodicallyChecker<M extends Instance, I extends InfoDto> implements Runnable {
        @Override
        public void run() {
            if (!clusterManagerConfig.getPeriodCheckSwitch()) {
                QUERY_INFO_LOGGER.info("[check][{}}] switch off， stop.", getName());
                return;
            }
            DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("drc.cm.checker", getName(), () -> {
                try {
                    // query info
                    long startTime = System.currentTimeMillis();
                    long endTime = startTime + clusterManagerConfig.getCheckMaxTime();
                    QUERY_INFO_LOGGER.info("[check][{}}] start query info", getName());
                    Map<String, List<M>> metaGroupByRegistryKeyMap = getMetaGroupByRegistryKeyMap();

                    List<Instance> instancesFromMeta = getAllMeta();
                    Pair<List<String>, List<I>> pair = fetchInstanceInfo(instancesFromMeta);
                    List<I> instanceList = pair.getValue();
                    Set<String> validIps = Sets.newHashSet(pair.getKey());
                    Set<String> invalidIps = instancesFromMeta.stream().map(Instance::getIp).filter(e -> !validIps.contains(e)).collect(Collectors.toSet());
                    QUERY_INFO_LOGGER.info("[check][{}] valid ip: {}. http fail ip: {}", getName(), validIps, invalidIps);

                    // check diff
                    doCheck(metaGroupByRegistryKeyMap, instanceList, validIps, startTime, endTime);
                    QUERY_INFO_LOGGER.info("[check][{}}] done", getName());
                } catch (Throwable e) {
                    EventMonitor.DEFAULT.logEvent("drc.cm.check.exception", getName());
                    QUERY_INFO_LOGGER.error("[check][{}}] exception. error:", getName(), e);
                }
            });
        }

        @VisibleForTesting
        void doCheck(Map<String, List<M>> metaGroupByRegistryKeyMap, List<I> allInstances, Set<String> validIps, long startTime, long endTime) {
            Map<String, List<I>> instanceGroupByRegistryKey = allInstances.stream().collect(Collectors.groupingBy(InfoDto::getRegistryKey));

            Set<String> registryKeys = this.getAllRegistryKey(metaGroupByRegistryKeyMap, instanceGroupByRegistryKey);
            for (String registryKey : registryKeys) {
                String clusterId = RegistryKey.from(registryKey).toString();

                if (this.shouldStopTask(startTime, endTime)) {
                    break;
                }

                if (!currentMetaManager.hasCluster(clusterId)) {
                    QUERY_INFO_LOGGER.info("{}: [ignore uninterested] clusterId: {}, registryKey: {}", getName(), clusterId, registryKeys);
                    EventMonitor.DEFAULT.logEvent(String.format("drc.cm.check.ignore.%s", getName()), registryKey);
                    continue;
                }

                List<M> metas = metaGroupByRegistryKeyMap.getOrDefault(registryKey, Collections.emptyList()).stream().filter(e -> validIps.contains(e.getIp())).collect(Collectors.toList());
                List<I> instances = instanceGroupByRegistryKey.getOrDefault(registryKey, Collections.emptyList());
                // 1. check extra instance (instance working but not in meta)
                Set<Instance> current = instances.stream().map(I::mapToInstance).collect(Collectors.toSet());
                Set<Instance> expected = this.mapToInstances(metas);
                List<Instance> instancesToRemove = this.getInstanceToRemove(current, expected);
                if (!CollectionUtils.isEmpty(instancesToRemove)) {
                    QUERY_INFO_LOGGER.info("{}: [delete redundant {}}]: \n" +
                            "current: {}, \n" +
                            "expected:{}, \n" +
                            "remove: {}", getName(), registryKey, current, expected, instancesToRemove);
                    if (clusterManagerConfig.getPeriodCorrectSwitch()) {
                        // do remove
                        for (Instance instance : instancesToRemove) {
                            removeRedundantInstance(registryKey, clusterId, instance);
                            EventMonitor.DEFAULT.logEvent(String.format("drc.cm.check.remove.%s", getName()), registryKey + ":" + instance.getIp());
                        }
                    } else {
                        for (Instance instance : instancesToRemove) {
                            EventMonitor.DEFAULT.logEvent(String.format("drc.cm.check.remove.%s.mock", getName()), registryKey + ":" + instance.getIp());
                        }
                    }
                    instancesToRemove.forEach(current::remove);
                }

                List<Instance> instanceToRegister = this.getInstanceToRegister(current, expected);
                List<M> metaToRegister = filterMeta(metas, instanceToRegister);
                if (!CollectionUtils.isEmpty(metaToRegister)) {
                    QUERY_INFO_LOGGER.info("{}: [register {}}]: \n" +
                            "current: {}, \n" +
                            "expected:{}, \n" +
                            "register: {}", getName(), registryKey, current, expected, instanceToRegister);
                    if (clusterManagerConfig.getPeriodCorrectSwitch()) {
                        // do remove
                        for (M instance : metaToRegister) {
                            registerInstance(clusterId, instance);
                            EventMonitor.DEFAULT.logEvent(String.format("drc.cm.check.register.%s", getName()), registryKey + ":" + instance.getIp());
                        }
                    } else {
                        for (Instance instance : instanceToRegister) {
                            EventMonitor.DEFAULT.logEvent(String.format("drc.cm.check.register.%s.mock", getName()), registryKey + ":" + instance.getIp());
                        }
                    }
                    // if register sucess, no need
                    continue;
                }


                // 2 check instance config (ip/port/isMaster) same
                if (!CollectionUtils.isEmpty(expected)) {
                    Replicator replicatorMaster = getReplicatorMaster(clusterId, metas);
                    Db dbMaster = getDbMaster(clusterId);
                    boolean upstreamIpMatch = isUpstreamIpMatch(replicatorMaster, dbMaster, instances);
                    boolean downStreamIpMatch = isDownStreamIpMatch(replicatorMaster, dbMaster, instances);
                    boolean instanceMatch = current.equals(expected);
                    if (instanceMatch && downStreamIpMatch && upstreamIpMatch) {
                        continue;
                    }
                    QUERY_INFO_LOGGER.info("{}: [refresh {}] instanceMatch:{}, upstreamIpMatch:{}, downStreamIpMatch:{}, \n" +
                            "expected: {}, \n" +
                            "current : {}", getName(), registryKey, instanceMatch, upstreamIpMatch, downStreamIpMatch, expected, current);
                    // refresh instancesMetaList if not consistent
                    if (clusterManagerConfig.getPeriodCorrectSwitch()) {
                        // do refresh
                        List<M> master = metas.stream().filter(M::getMaster).collect(Collectors.toList());
                        if (CollectionUtils.isEmpty(master) || master.size() != 1) {
                            QUERY_INFO_LOGGER.info("{}: [refresh {}] fail", getName(), registryKey);
                            EventMonitor.DEFAULT.logEvent(String.format("drc.cm.check.refresh.%s.fail", getName()), registryKey);
                            continue;
                        }
                        refreshInstance(clusterId, master.get(0));
                        EventMonitor.DEFAULT.logEvent(String.format("drc.cm.check.refresh.%s", getName()), registryKey);
                    } else {
                        EventMonitor.DEFAULT.logEvent(String.format("drc.cm.check.refresh.%s.mock", getName()), registryKey);
                    }
                }
            }
        }

        private boolean shouldStopTask(long startTime, long endTime) {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis > endTime) {
                QUERY_INFO_LOGGER.info("{}: [timeout] currentTimeMillis: {}, startTime: {}, endTime:{}", getName(), currentTimeMillis, startTime, endTime);
                EventMonitor.DEFAULT.logEvent("drc.cm.check.timeout", getName());
                return true;
            }

            long lastChildEventTime = clusterServers.getLastChildEventTime();
            if (lastChildEventTime > startTime) {
                QUERY_INFO_LOGGER.info("{}: [possible reshard] currentTimeMillis: {}, startTime: {}, lastChildEventTime: {}", getName(), currentTimeMillis, startTime, lastChildEventTime);
                EventMonitor.DEFAULT.logEvent("drc.cm.check.reshard.skip", getName());
                return true;
            }
            return false;
        }

        protected Set<String> getAllRegistryKey(Map<String, List<M>> metaGroupByRegistryKeyMap, Map<String, List<I>> instanceGroupByRegistryKey) {
            Set<String> registryKeysFromMeta = metaGroupByRegistryKeyMap.keySet();
            Set<String> clusterIdFromMetaSet = registryKeysFromMeta.stream().map(registryKey -> RegistryKey.from(registryKey).toString()).collect(Collectors.toSet());
            Set<String> registryKeysFromInstance = instanceGroupByRegistryKey.keySet().stream()
                    .filter(registryKey -> {
                        String clusterId = RegistryKey.from(registryKey).toString();
                        return currentMetaManager.hasCluster(clusterId) && clusterIdFromMetaSet.contains(clusterId);
                    }).collect(Collectors.toSet());
            Set<String> registryKeys = new HashSet<>();
            registryKeys.addAll(registryKeysFromInstance);
            registryKeys.addAll(registryKeysFromMeta);
            return registryKeys;
        }

        private Db getDbMaster(String clusterId) {
            Endpoint mySQLMaster = currentMetaManager.getMySQLMaster(clusterId);
            if (mySQLMaster == null) {
                return null;
            }
            return new Db().setMaster(true).setIp(mySQLMaster.getHost()).setPort(mySQLMaster.getPort());
        }

        protected boolean isDownStreamIpMatch(Replicator replicatorMaster, Db dbMaster, List<I> instances) {
            return true;
        }

        protected boolean isUpstreamIpMatch(Replicator replicatorMaster, Db dbMaster, List<I> instanceInstances) {
            boolean instanceMasterMatch = true;
            if (replicatorMaster != null) {
                String masterIp = replicatorMaster.getIp();
                instanceMasterMatch = instanceInstances.stream().filter(I::getMaster).allMatch(e -> masterIp.equals(e.getUpstreamIp()));
            }
            return instanceMasterMatch;
        }

        private Set<Instance> mapToInstances(List<M> instanceMetas) {
            return instanceMetas.stream().map(e -> new SimpleInstance().setIp(e.getIp()).setPort(e.getPort()).setMaster(e.getMaster())).collect(Collectors.toSet());
        }

        private List<Instance> getInstanceToRemove(Set<Instance> current, Set<Instance> expected) {
            if (current.equals(expected)) {
                // same, do nothing
                return Lists.newArrayList();
            }
            return current.stream().filter(e -> expected.stream().noneMatch(t -> t.getIp().equals(e.getIp()) && t.getPort().equals(e.getPort()))).collect(Collectors.toList());
        }

        private List<Instance> getInstanceToRegister(Set<Instance> current, Set<Instance> expected) {
            if (current.equals(expected)) {
                // same, do nothing
                return Lists.newArrayList();
            }
            return expected.stream().filter(e -> current.stream().noneMatch(t -> t.getIp().equals(e.getIp()) && t.getPort().equals(e.getPort()))).collect(Collectors.toList());
        }


        private List<M> filterMeta(List<M> metas, List<Instance> instanceToRegister) {
            return metas.stream().filter(meta -> instanceToRegister.stream().anyMatch(instance -> instance.getIp().equals(meta.getIp()) && instance.getPort().equals(meta.getPort()))).collect(Collectors.toList());
        }

        abstract void registerInstance(String clusterId, M instance);

        abstract void refreshInstance(String clusterId, M master);

        protected abstract Pair<List<String>, List<I>> fetchInstanceInfo(List<Instance> instances);

        protected abstract List<Instance> getAllMeta();

        protected abstract Map<String, List<M>> getMetaGroupByRegistryKeyMap();

        protected abstract String getName();

        protected abstract Replicator getReplicatorMaster(String clusterId, List<M> instanceMetas);

        protected abstract void removeRedundantInstance(String registryKey, String clusterId, Instance instance);
    }
}