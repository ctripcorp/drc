package com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl.ghost;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl.ghost.GhostDdlPairCase.DATABASE;

/**
 * @Author limingdong
 * @create 2020/3/20
 */
public class GhostExecutor {

    private static final Logger logger = LoggerFactory.getLogger(GhostExecutor.class);

    public static void execute(String command) throws Exception {
        Process ghost = null;
        InputStream is = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        try {
            String[] commands = command.split("&&");
            ghost = Runtime.getRuntime().exec(commands);
            boolean exitValue = ghost.waitFor(120, TimeUnit.SECONDS);
            is = ghost.getErrorStream();
            isr = new InputStreamReader(is);
            br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                logger.info(line);
                if (line.contains("address already in use")) {
                    deleteSocket();
                }
            }

            logger.info("[Execute] ghost command {} with exit value {}", StringUtils.join(commands, " "), exitValue);
            ghost.destroy();
        } catch (Exception e) {
            logger.error("execute command [{}] error", command, e);
        } finally {
            if (ghost != null) {
                ghost.destroy();
            }
            if (is != null) {
                is.close();
            }
            if (isr != null) {
                isr.close();
            }
            if (br != null) {
                br.close();
            }
        }
    }


    public static void deleteSocket() {
        File file = new File(String.format("/tmp/gh-ost.%s.t1.sock", DATABASE));
        if (file.exists()) {
            file.delete();
            logger.info("delete file {}", file.getName());
        }
    }

}
