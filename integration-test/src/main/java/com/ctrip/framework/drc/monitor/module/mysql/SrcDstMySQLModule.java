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

    private String srcImage;
    private String destImage;


    public SrcDstMySQLModule(int srcMySQLPort, int destMySQLPort,String image) {
        this.srcMySQLPort = srcMySQLPort;
        this.destMySQLPort = destMySQLPort;
        this.srcImage = image;
        this.destImage = image;
    }
    public  SrcDstMySQLModule(int srcMySQLPort, int destMySQLPort, String srcImage,String destImage) {
        this.srcMySQLPort = srcMySQLPort;
        this.destMySQLPort = destMySQLPort;
        this.srcImage = srcImage;
        this.destImage = destImage;
    }



    @Override
    protected void doInitialize() throws Exception{
        startMySQL();
    }

    private void startMySQL() throws Exception {
        if (isUsed(srcMySQLPort)) {
            InstanceConfig srcConfig = new InstanceConfig(srcMySQLPort, "sourcemysqltest", "src/", srcImage);
            src = new DockerInstance(srcConfig);
            src.start();
        }

        if (isUsed(destMySQLPort)) {
            InstanceConfig dstConfig = new InstanceConfig(destMySQLPort, "destmysqltest", "dst/", destImage);
            dst = new DockerInstance(dstConfig);
            dst.start();
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
