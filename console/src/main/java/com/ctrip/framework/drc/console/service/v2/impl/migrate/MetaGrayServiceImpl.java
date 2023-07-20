package com.ctrip.framework.drc.console.service.v2.impl.migrate;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.MhaGrayConfig;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.service.v2.MetaCompareService;
import com.ctrip.framework.drc.console.service.v2.MetaGrayService;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @ClassName MetaGrayServiceImpl
 * @Author haodongPan
 * @Date 2023/6/28 14:56
 * @Version: $
 */
@Service
public class MetaGrayServiceImpl extends AbstractMonitor implements MetaGrayService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired private MetaProviderV2 metaProviderV2;

    @Autowired private DbClusterSourceProvider metaProviderV1;

    @Autowired private DefaultConsoleConfig consoleConfig;

    @Autowired private MhaGrayConfig mhaGrayConfig;
    
    @Autowired private MetaCompareService metaCompareService;
    
    private volatile Drc grayDrc;


    @Override
    public Drc getDrc(String dcId)  {
        Dc dc = getDrc().findDc(dcId);
        Drc drcWithOneDc = new Drc();
        drcWithOneDc.addDc(dc);
        return drcWithOneDc;
    }

    @Override
    public Drc getDrc()  {
        if (null == grayDrc) {
            scheduledTask();
        }
        return grayDrc;
    }

    @Override
    public DbClusterCompareRes compareDbCluster(String dbClusterId) {
        return metaCompareService.compareDbCluster(dbClusterId);
    }

    @Override
    public String compareDrcMeta() throws Exception {
        return metaCompareService.compareDrcMeta();
    }

    @Override
    public void initialize() {
        setInitialDelay(30);
        setPeriod(30);
        setTimeUnit(TimeUnit.SECONDS);
        super.initialize();
    }

    @Override
    public synchronized void scheduledTask() {
        try {
            long start = System.currentTimeMillis();
            logger.info("[[tag=metaGray]] refreshDrc start");
            grayDrc = refreshDrc();
            logger.info("[[tag=metaGray]] refreshDrc end,cost:{}",System.currentTimeMillis() - start);
        } catch (Throwable t) {
            logger.error("[[tag=metaGray]] refresh gray drc fail", t);
        }
    }

    protected synchronized Drc refreshDrc()  {
        Drc oldDrc = metaProviderV1.getDrc();
        Set<String> publicCloudRegion = consoleConfig.getPublicCloudRegion();
        String localRegion = consoleConfig.getRegion();
        if (publicCloudRegion.contains(localRegion)) { //cloud region use remoteConfig,which already recombination
            return oldDrc;
        }
        try {
            if (mhaGrayConfig.getDbClusterGraySwitch()  && metaCompareService.isConsistent()) {
                Drc drcCopy = DefaultSaxParser.parse(oldDrc.toString());
                int memoryAddress1 = System.identityHashCode(oldDrc);
                int memoryAddress2 = System.identityHashCode(drcCopy);
                boolean anotherObject = !(drcCopy == oldDrc) && !(memoryAddress1 == memoryAddress2);
                logger.info("[[tag=metaGray]] drc deep copy res:{}",anotherObject);
                for (String dbClusterId : mhaGrayConfig.getGrayDbClusterSet()) {
                    Dc newDc = metaProviderV2.getDcBy(dbClusterId);
                    DbCluster newDcDbCluster = newDc.findDbCluster(dbClusterId);
                    
                    Dc dcCopy = drcCopy.findDc(newDc.getId());
                    logger.info("[[tag=metaGray]] gray dbClusterId:{},oldDbCluster:{},newDbCluster:{}",dbClusterId,
                            dcCopy.findDbCluster(dbClusterId),newDcDbCluster);
                    dcCopy.removeDbCluster(dbClusterId);
                    dcCopy.addDbCluster(newDcDbCluster);
                }
                return drcCopy;
            } else {
                logger.info("[[tag=metaGray]] use old meta");
            }
        } catch (Throwable e) {
            logger.error("[[tag=metaGray]] gray new meta fail,use old meta", e);
        }
        return oldDrc;
    }

}
