package com.ctrip.framework.drc.console.utils.analysis;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.FileUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map.Entry;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 * @ClassName MetaAnalysisUtils
 * @Author haodongPan
 * @Date 2024/1/15 11:17
 * @Version: $
 */
public class MetaAnalysisUtils {

    @Test
    public void test() throws IOException, SAXException {
        // get json from api :/api/drc/v1/openapi/info/dbs
        InputStream ins = FileUtils.getFileInputStream("analysis/meta.xml");
        String metaXml = DefaultSaxParser.parse(ins).toString();
        Drc drc = DefaultSaxParser.parse(metaXml);
        dbClusterWithOutReplicatorAndApplier(drc);
    }
    
    private void dbClusterWithOutReplicatorAndApplier(Drc drc) {
        int count = 0;
        for (Entry<String, Dc> dcEntry : drc.getDcs().entrySet()) {
            Dc dc = dcEntry.getValue();
            for (Entry<String, DbCluster> dbClusterEntry : dc.getDbClusters().entrySet()) {
                DbCluster dbCluster = dbClusterEntry.getValue();
                List<Replicator> replicators = dbCluster.getReplicators();
                List<Applier> appliers = dbCluster.getAppliers();
                List<Messenger> messengers = dbCluster.getMessengers();
                if (replicators.isEmpty() && appliers.isEmpty() && messengers.isEmpty()) {
                    count++;
                    System.out.println(dbCluster.getMhaName());
                }
            }
        }
        
    } 
}
