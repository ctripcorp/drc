package com.ctrip.framework.drc.console.utils;

import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;

import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-11
 */
public class XmlUtil {

    public static Drc extractDrc(Drc roughDrc) {
        Drc drc = new Drc();
        Map<String, Dc> dcs = roughDrc.getDcs();
        for (String dcName : dcs.keySet()) {
            Dc dc = new Dc(dcName);
            drc.addDc(dc);
            dcs.get(dcName).getDbClusters().forEach((s, dbCluster) -> dc.addDbCluster(dbCluster));
        }
        return drc;
    }
}
