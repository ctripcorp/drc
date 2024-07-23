package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.config.AbstractConfigBean;

import java.io.FileOutputStream;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.service.utils.Constants.MEMORY_META_SERVER_DAO_KEY;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;

public abstract class AbstractConfig extends AbstractConfigBean implements Config{

    protected volatile String xml;
    protected volatile Drc drc;

    private ExecutorService executorService = ThreadUtils.newSingleThreadExecutor("persist-config");

    @Override
    public String getConfig() {
        if(this.xml == null) {
            updateConfig();
        }
        if (this.xml == null) {
            throw new IllegalStateException("drc config is null");
        }
        return this.xml;
    }

    @Override
    public Drc getDrc() {
        if (this.drc == null) {
            updateConfig();
        }
        if (this.drc == null) {
            throw new IllegalStateException("drc is null");
        }
        return this.drc;
    }

    @Override
    public String getSourceType() {
        return this.getClass().getSimpleName();
    }

    protected void persistConfig() {
        executorService.submit(() -> {
            try {
                String fileName = System.getProperty(MEMORY_META_SERVER_DAO_KEY, "memory_meta_server_dao_file.xml");
                META_LOGGER.info("[persist config][write file]{}", fileName);
                FileOutputStream out = new FileOutputStream("./" + fileName);
                out.write(xml.getBytes());
                out.close();
            } catch (Throwable t) {
                META_LOGGER.error("Fail persist config: {}", xml, t);
            }
        });
    };
}
