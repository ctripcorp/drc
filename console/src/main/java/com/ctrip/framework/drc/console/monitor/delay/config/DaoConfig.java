package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;

@Component
@Order(1)
public class DaoConfig extends AbstractConfig implements Config{

    @Autowired
    private MetaGenerator metaGenerator;
    
    @Autowired
    private DataCenterService dataCenterService;

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Override
    public void updateConfig() {
        try {
            String localDc = dataCenterService.getDc();
            Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
            if (publicCloudDc.contains(localDc))  return;
            Drc drc = metaGenerator.getDrc();
            META_LOGGER.debug("[meta] generated drc: {}", drc);

            if (null != drc && drc.getDcs().size() != 0 && !drc.toString().equalsIgnoreCase(this.xml)) {
                this.xml = drc.toString();
                persistConfig();
            }
        } catch (Exception e) {
            META_LOGGER.error("Fail get drc from db, ", e);
        }
    }

    protected List<String> addDbClusterAfterFilter(Drc drc, Drc drcSource, List<String> filterOutMhas, boolean peripheral) {
        List<String> addedMhas = Lists.newArrayList();
        if (null != drcSource) {
            Map<String, Dc> dcsSource = drcSource.getDcs();
            for (String dcId : dcsSource.keySet()) {
                Dc dc = drc.getDcs().get(dcId);
                if(null == dc) {
                    dc = new Dc(dcId);
                    drc.addDc(dc);
                }

                Dc dcSource = dcsSource.get(dcId);
                // add peripheral config like cm, zk and proxy route
                if (peripheral) {
                    dcSource.getClusterManagers().forEach(dc::addClusterManager);
                    dc.setZkServer(dcSource.getZkServer());
                    dcSource.getRoutes().forEach(dc::addRoute);
                }

                for (DbCluster dbCluster : dcSource.getDbClusters().values()) {
                    String mhaName = dbCluster.getMhaName();
                    if (!filterOutMhas.contains(mhaName)) {
                        dc.addDbCluster(dbCluster);
                        addedMhas.add(mhaName);
                    }
                }
            }
        }
        return addedMhas;
    }

}
