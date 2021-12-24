package com.ctrip.framework.drc.monitor.module.mysql;

import com.ctrip.framework.drc.monitor.module.config.AbstractConfigTest;
import com.ctrip.xpipe.api.lifecycle.Destroyable;

/**
 * Created by mingdongli
 * 2019/10/15 上午12:56.
 */
public class SrcDstMySQLModule extends AbstractConfigTest implements Destroyable {

    private DockerInstance src;

    private DockerInstance dst;

    private DockerInstance meta;

    private int srcMySQLPort;

    private int destMySQLPort;

    private int metaMySQLPort;

    private String image;

    public SrcDstMySQLModule(int srcMySQLPort, int destMySQLPort, int metaMySQLPort, String image) {
        this.srcMySQLPort = srcMySQLPort;
        this.destMySQLPort = destMySQLPort;
        this.metaMySQLPort = metaMySQLPort;
        this.image = image;
    }

    @Override
    protected void doInitialize() throws Exception{
        startMySQL();
    }

    private void startMySQL() throws Exception {
        if (isUsed(srcMySQLPort)) {
            InstanceConfig srcConfig = new InstanceConfig(srcMySQLPort, "sourcemysqltest", "src/", image);
            src = new DockerInstance(srcConfig);
            src.start();
        }

        if (isUsed(destMySQLPort)) {
            InstanceConfig dstConfig = new InstanceConfig(destMySQLPort, "destmysqltest", "dst/", image);
            dst = new DockerInstance(dstConfig);
            dst.start();
        }

        if (isUsed(metaMySQLPort)) {
            InstanceConfig metaConfig = new InstanceConfig(metaMySQLPort, "metamysqltest", "meta/", image);
            meta = new DockerInstance(metaConfig);
            meta.start();
        }
    }

    @Override
    protected void doStop() throws Exception{
        if (src != null) {
            src.stop();
        }
        if (dst != null) {
            dst.stop();
        }
    }

    @Override
    public void destroy() throws Exception {
        Process rm = Runtime.getRuntime().exec(InstanceConfig.getPruneCommand());
        rm.waitFor();
        logger.info("[{}] execute docker volume prune", getClass().getSimpleName());
    }

}
