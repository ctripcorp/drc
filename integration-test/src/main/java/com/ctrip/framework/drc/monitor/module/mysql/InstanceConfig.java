package com.ctrip.framework.drc.monitor.module.mysql;

/**
 * Created by mingdongli
 * 2019/10/11 下午2:15.
 */
public class InstanceConfig {

    private static final String DEFAULT_IMAGE = "mysql:5.7";

    /**
     * 对外暴露的端口
     * 本地my.cnf路径
     * 镜像名称，需要本地已经下载镜像
     */
    private static final String COMMAND = "docker run -p %d:%d --name %s -v %smy.cnf:/etc/mysql/conf.d/my.cnf -e MYSQL_ROOT_PASSWORD=root -d %s --port=%d";

    private int port;

    private String confPath;

    private String name;

    private String image;

    public InstanceConfig(int port, String name, String confPath) {
        this(port, name, confPath, DEFAULT_IMAGE);
    }

    public InstanceConfig(int port, String name, String confPath, String image) {
        this.port = port;
        this.name = name + "_" + port;
        this.confPath = confPath;
        this.image = image;
    }

    public String[] getRunCommand() {
        String absolutePath = this.getClass().getClassLoader().getResource(confPath).getPath();
        String command = String.format(COMMAND, getPort(), getPort(), getName(), absolutePath, getImage(), getPort());
        String[] commands = command.split(" ");
        return commands;
    }

    public String[] getStopCommand() {
        return new String[]{"docker", "stop", name};
    }

    public String[] getRmCommand() {
        return new String[]{"docker", "rm", name};
    }

    public static String[] getPruneCommand() {
        return new String[]{"docker", "volume", "prune", "-f"};
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getConfPath() {
        return confPath;
    }

    public void setConfPath(String confPath) {
        this.confPath = confPath;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
