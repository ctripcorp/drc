package com.ctrip.framework.drc.core.server.config;

import com.ctrip.xpipe.config.AbstractConfig;
import com.ctrip.xpipe.utils.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DEFAULT_CONFIG_FILE_NAME;

/**
 * @Author limingdong
 * @create 2021/12/1
 */
public class DefaultFileConfig extends AbstractConfig {

    private Properties properties = new Properties();

    private static String DEFAULT_LOCAL_PATH = System.getProperty("localpath", "/opt/settings/drc");
    private static String DEFAULT_CONFIG_FILE = System.getProperty("configFile", DEFAULT_CONFIG_FILE_NAME);


    public DefaultFileConfig(){
        this(DEFAULT_CONFIG_FILE);
    }

    public DefaultFileConfig(String file) {
        this(DEFAULT_LOCAL_PATH, file);
    }

    public DefaultFileConfig(String localPath, String file) {

        properties.putAll(load(localPath, file));
    }

    private Properties load(String localPath, String file) {

        Properties properties = new Properties();
        InputStream ins = null;
        try {
            ins = FileUtils.getFileInputStream(localPath, file);
            properties.load(ins);
        } catch (IOException e) {
            throw new IllegalArgumentException("file io exception:" + localPath + "," + file, e);
        }finally{
            if(ins != null){
                try {
                    ins.close();
                } catch (IOException e) {
                    logger.error("[load]" + localPath + "," + file, e);
                }
            }
        }
        return properties;
    }

    @Override
    public String get(String key) {
        return properties.getProperty(key);
    }
}
