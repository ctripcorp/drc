package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.xpipe.rest.ForwardType;
import com.ctrip.xpipe.spring.AbstractController;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.LinkedList;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public enum META_SERVER_SERVICE {
    //common
    GET_ACTIVE_REPLICATOR(PATH.GET_ACTIVE_REPLICATOR, ForwardType.FORWARD),

    GET_ACTIVE_MYSQL(PATH.GET_ACTIVE_MYSQL, ForwardType.FORWARD),

    //console
    CLUSTER_CHANGE(PATH.PATH_CLUSTER_CHANGE, ForwardType.MULTICASTING),

    //multi dc
    UPSTREAM_CHANGE(PATH.PATH_UPSTREAM_CHANGE, ForwardType.FORWARD);

    private String path;
    private ForwardType forwardType;

    META_SERVER_SERVICE(String path, ForwardType forwardType) {
        this.path = path;
        this.forwardType = forwardType;
    }

    public String getPath() {
        return path;
    }

    public ForwardType getForwardType() {
        return forwardType;
    }

    public String getRealPath(String host) {

        if (!host.startsWith("http")) {
            host = "http://" + host;
        }
        return String.format("%s%s%s", host, PATH.PATH_PREFIX, getPath());
    }


    protected boolean match(String realPath) {

        String[] realsp = StringUtil.splitRemoveEmpty("\\/+", realPath);
        String[] sp = StringUtil.splitRemoveEmpty("\\/+", getPath());

        if (sp.length != realsp.length) {
            return false;
        }
        for (int i = 0; i < sp.length; i++) {

            if (sp[i].startsWith("{")) {
                continue;
            }
            if (!realsp[i].equalsIgnoreCase(sp[i])) {
                return false;
            }
        }
        return true;
    }

    public static META_SERVER_SERVICE fromPath(String path) {
        return fromPath(path, PATH.PATH_PREFIX);
    }

    protected static META_SERVER_SERVICE fromPath(String path, String prefix) {

        List<META_SERVER_SERVICE> matched = new LinkedList<>();
        if (prefix != null && path.startsWith(prefix)) {
            path = path.substring(prefix.length());
        }

        for (META_SERVER_SERVICE service : META_SERVER_SERVICE.values()) {
            if (service.match(path)) {
                matched.add(service);
            }
        }
        if (matched.size() == 1) {
            return matched.get(0);
        }
        throw new IllegalStateException("from path:" + path + ", we found matches:" + matched);
    }

    public static class PATH extends AbstractController {

        public static final String BACKUP_CLUSTER_ID_PATH_VARIABLE = "{backupClusterId:.+}";

        public static final String PATH_PREFIX = "/api/meta";

        //common
        public static final String GET_ACTIVE_REPLICATOR = "/getactivereplicator/" + CLUSTER_ID_PATH_VARIABLE + "/";

        public static final String GET_ACTIVE_MYSQL = "/getactivemysql/" + CLUSTER_ID_PATH_VARIABLE + "/";

        //console
        public static final String PATH_CLUSTER_CHANGE = "/clusterchange/" + CLUSTER_ID_PATH_VARIABLE + "/";

        //multi dc
        public static final String PATH_UPSTREAM_CHANGE = "/upstreamchange/" + BACKUP_CLUSTER_ID_PATH_VARIABLE + "/" + CLUSTER_ID_PATH_VARIABLE + "/{ip}/{port}";  //src clusterId, dst clusterId

    }

}
