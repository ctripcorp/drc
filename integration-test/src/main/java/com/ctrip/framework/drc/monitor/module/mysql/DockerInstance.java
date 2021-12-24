package com.ctrip.framework.drc.monitor.module.mysql;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by mingdongli
 * 2019/10/11 下午2:07.
 */
public class DockerInstance implements Startable, Stoppable {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private InstanceConfig config;

    private Process mySQL;

    public DockerInstance(InstanceConfig config) {
        this.config = config;
    }

    @Override
    public void start() throws Exception {
        BufferedReader br = null;
        try {
            String[] commands = config.getRunCommand();
            mySQL = Runtime.getRuntime().exec(commands);
            mySQL.waitFor();
            InputStream is = mySQL.getErrorStream();
            InputStreamReader isr = new InputStreamReader(is);
            br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                logger.error(line);
            }

            logger.info("start mySQL with {}", StringUtils.join(commands, " "));
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    @Override
    public void stop() throws Exception {
        Process stop = Runtime.getRuntime().exec(config.getStopCommand());
        stop.waitFor();
        Process rm = Runtime.getRuntime().exec(config.getRmCommand());
        rm.waitFor();
        mySQL.destroy();
        logger.info("stop mySQL successfully");
    }

}
