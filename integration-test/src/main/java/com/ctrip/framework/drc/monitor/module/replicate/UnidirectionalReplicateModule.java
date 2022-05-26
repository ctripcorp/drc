package com.ctrip.framework.drc.monitor.module.replicate;

import com.ctrip.framework.drc.monitor.DrcMonitorModule;
import com.ctrip.framework.drc.monitor.module.ReplicateModule;
import com.ctrip.framework.drc.monitor.module.mysql.SrcDstMySQLModule;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

import static com.ctrip.framework.drc.monitor.module.config.AbstractConfigTest.*;

/**
 * Created by mingdongli
 * 2019/10/15 上午1:38.
 */
public class UnidirectionalReplicateModule extends AbstractLifecycle implements ReplicateModule {

    private SrcDstMySQLModule mySQLModule;

    private ReplicatorApplierPairModule replicatorApplierPairModule;

    private DrcMonitorModule drcMonitorModule;

    protected int srcMySQLPort;

    protected int destMySQLPort;

    protected int metaMySQLPort;

    private int repPort;

    private String registryKey;

    public void setImage(String image) {
        this.image = image;
    }

    private String image = "mysql:5.7";

    private boolean needRowFilter = false;

    private static final String ROW_FILTER_PROPERTIES = "{" +
            "  \"rowsFilters\": [" +
            "    {" +
            "      \"mode\": \"trip_uid\"," +
            "      \"tables\": \"(CommonOrderShard[1-9]DB|CommonOrderShard[1][0-2]DB)\\\\.(basicorder|basicorder_ext)\"," +
            "      \"parameters\": {" +
            "        \"columns\": [" +
            "          \"UID\"" +
            "        ]," +
            "        \"illegalArgument\": false," +
            "        \"context\": \"SIN\"" +
            "      }" +
            "    }" +
            "  ]" +
            "}";


    private static final String ROW_FILTER_PROPERTIES_REGEX = "{" +
            "  \"rowsFilters\": [" +
            "    {" +
            "      \"mode\": \"java_regex\"," +
            "      \"tables\": \".*\"," +
            "      \"parameters\": {" +
            "        \"columns\": [" +
            "          \"id\"" +
            "        ]," +
            "        \"context\": \"^\\\\d*[02468]$\"" +
            "      }" +
            "    }" +
            "  ]" +
            "}";

    public UnidirectionalReplicateModule() {
        this(SOURCE_MASTER_PORT, DESTINATION_MASTER_PORT, META_PORT, REPLICATOR_MASTER_PORT, REGISTRY_KEY);
    }

    public UnidirectionalReplicateModule(int srcMySQLPort, int destMySQLPort, int metaMySQLPort, int repPort, String registryKey) {
        this.srcMySQLPort = srcMySQLPort;
        this.destMySQLPort = destMySQLPort;
        this.metaMySQLPort = metaMySQLPort;
        this.repPort = repPort;
        this.registryKey = registryKey;
    }

    @Override
    protected void doStop() throws Exception{
        replicatorApplierPairModule.stop();
        mySQLModule.stop();
    }

    @Override
    protected void doDispose() throws Exception {
        drcMonitorModule.dispose();
        replicatorApplierPairModule.dispose();
        mySQLModule.dispose();
    }

    @Override
    public void destroy() throws Exception {
        drcMonitorModule.destroy();
        replicatorApplierPairModule.destroy();
        mySQLModule.destroy();
    }



    @Override
    public void startMonitorModule() {
        try {
            drcMonitorModule.start();
        } catch (Exception e) {
            logger.error("startMonitorModule error", e);
        }
        logger.info("<<<<<<<<<<<< DefaultMonitorManagerTest module started");

    }

    @Override
    public void stopMonitorModule() {
        try {
            drcMonitorModule.stop();
        } catch (Exception e) {
            logger.error("stopMonitorModule error", e);
        }
    }



    @Override
    public void startMySQLModule() {
        try {
            mySQLModule = new SrcDstMySQLModule(srcMySQLPort, destMySQLPort, metaMySQLPort, image);
            mySQLModule.initialize();
            mySQLModule.start();

            //这里初试化数据库表结构
            drcMonitorModule = getDrcMonitorModule();
            drcMonitorModule.initialize();
        } catch (Exception e) {
            logger.error("startMySQLModule error", e);
        }
        logger.info("<<<<<<<<<<<< MySQL module started");
    }

    @Override
    public void startRAModule() {
        try {
            replicatorApplierPairModule = new ReplicatorApplierPairModule(srcMySQLPort, destMySQLPort, repPort, registryKey, needRowFilter ? ROW_FILTER_PROPERTIES : null);
            replicatorApplierPairModule.initialize();
            replicatorApplierPairModule.start();
        } catch (Exception e) {
            logger.error("startRAModule error", e);
        }
        logger.info("<<<<<<<<<<<< RA module started");
    }

    protected DrcMonitorModule getDrcMonitorModule() {
        return new DrcMonitorModule(srcMySQLPort, destMySQLPort, metaMySQLPort);
    }
}
