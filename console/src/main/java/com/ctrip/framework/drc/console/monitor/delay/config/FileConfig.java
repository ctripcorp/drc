package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

import static com.ctrip.framework.drc.core.service.utils.Constants.MEMORY_META_SERVER_DAO_KEY;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;

@Component
@Order(3)
public class FileConfig extends AbstractConfig implements Config{

    @Override
    public void updateConfig() {
        String fileName = System.getProperty(MEMORY_META_SERVER_DAO_KEY, "memory_meta_server_dao_file.xml");
        META_LOGGER.info("[loadDrc][load from file]{}", fileName);
        try {
            InputStream ins = FileUtils.getFileInputStream(fileName);
            String tempC = DefaultSaxParser.parse(ins).toString();
            if (StringUtils.isNotBlank(tempC)) {
                this.xml = tempC;
            }
        } catch (SAXException | IOException e) {
            META_LOGGER.error("[load]" + fileName, e);
        }
    }
}
