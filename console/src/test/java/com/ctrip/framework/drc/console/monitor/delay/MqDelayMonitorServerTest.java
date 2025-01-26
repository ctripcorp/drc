package com.ctrip.framework.drc.console.monitor.delay;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.mq.DelayMessageConsumer;
import com.ctrip.framework.drc.core.mq.IKafkaDelayMessageConsumer;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Map;
import java.util.Set;


public class MqDelayMonitorServerTest {

    @InjectMocks
    private MqDelayMonitorServer mqDelayMonitorServer;

    @Mock
    private MonitorTableSourceProvider monitorProvider;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private DelayMessageConsumer consumer;
    @Mock
    private IKafkaDelayMessageConsumer kafkaConsumer;
    @Mock
    private MetaProviderV2 metaProviderV2;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(monitorProvider.getMqDelayMonitorSwitch()).thenReturn("on");
        Mockito.when(monitorProvider.getKafkaDelayMonitorSwitch()).thenReturn("on");
        Mockito.when(monitorProvider.getMqDelaySubject()).thenReturn("bbz.drc.delaymonitor");
        Mockito.when(monitorProvider.getMqDelayConsumerGroup()).thenReturn("100023928");
        Mockito.when(consoleConfig.getDcsInLocalRegion()).thenReturn(Set.of("shaxy"));
        Mockito.when(consumer.resumeListen()).thenReturn(true);
        Mockito.when(consumer.stopListen()).thenReturn(true);
        Mockito.when(metaProviderV2.getDrc()).thenReturn(buildDrc());
        Mockito.when(consoleConfig.getDcsInLocalRegion()).thenReturn(Sets.newHashSet("ntgxh","shaxy","sharb"));
    }

    private Drc buildDrc() throws IOException, SAXException {
        return DefaultSaxParser.parse(getXml());
    }

    private static String getXml() {
        return "<drc>\n" +
                "   <dc id=\"ntgxh\" region=\"ntgxh\">\n" +
                "      <dbClusters>\n" +
                "         <dbCluster id=\"ob_zyn_test_dalcluster.ob_zyn_test\" name=\"ob_zyn_test_dalcluster\" mhaName=\"ob_zyn_test\" buName=\"BBZ\" org-id=\"3\" appId=\"-1\" applyMode=\"1\">\n" +
                "            <dbs>\n" +
                "               <db ip=\"10.119.118.18\" port=\"2883\" master=\"true\" uuid=\"3e0c9823-4da0-11ef-ad38-fa163eb2f6d2\"/>\n" +
                "            </dbs>\n" +
                "            <replicator ip=\"10.118.1.141\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"3e0c9823-4da0-11ef-ad38-fa163eb2f6d2:1-11\" master=\"true\" excludedTables=\"\"/>\n" +
                "            <messenger ip=\"10.118.1.127\" port=\"8080\" gtidExecuted=\"3e0c9823-4da0-11ef-ad38-fa163eb2f6d2:1-11\" master=\"false\" applyMode=\"2\" nameFilter=\"bbzobdaltestdb\\..*\" properties=\"{&quot;nameFilter&quot;:&quot;bbzobdaltestdb\\\\..*&quot;,&quot;dataMediaConfig&quot;:{&quot;rowsFilters&quot;:[],&quot;ruleFactory&quot;:{},&quot;table2Config&quot;:{},&quot;table2ColumnConfig&quot;:{},&quot;table2Filter&quot;:{},&quot;table2ColumnFilter&quot;:{},&quot;table2Rule&quot;:{},&quot;table2ColumnRule&quot;:{},&quot;matchResult&quot;:{&quot;isolateCache&quot;:{}},&quot;matchColumnsResult&quot;:{&quot;isolateCache&quot;:{}}},&quot;mqConfigs&quot;:[{&quot;table&quot;:&quot;bbzobdaltestdb\\\\..*&quot;,&quot;topic&quot;:&quot;bbz.obtest.binlog&quot;,&quot;mqType&quot;:&quot;qmq&quot;,&quot;serialization&quot;:&quot;json&quot;,&quot;persistent&quot;:false,&quot;order&quot;:false,&quot;delayTime&quot;:0}],&quot;regex2Configs&quot;:{},&quot;regex2Filter&quot;:{},&quot;regex2Producers&quot;:{},&quot;tableName2Producers&quot;:{}}\"/>\n" +
                "            <messenger ip=\"10.118.1.127\" port=\"8080\" gtidExecuted=\"3e0c9823-4da0-11ef-ad38-fa163eb2f6d2:1-11\" master=\"false\" applyMode=\"5\" nameFilter=\"bbzobdaltestdb\\..*\" properties=\"{&quot;nameFilter&quot;:&quot;bbzobdaltestdb\\\\..*&quot;,&quot;dataMediaConfig&quot;:{&quot;rowsFilters&quot;:[],&quot;ruleFactory&quot;:{},&quot;table2Config&quot;:{},&quot;table2ColumnConfig&quot;:{},&quot;table2Filter&quot;:{},&quot;table2ColumnFilter&quot;:{},&quot;table2Rule&quot;:{},&quot;table2ColumnRule&quot;:{},&quot;matchResult&quot;:{&quot;isolateCache&quot;:{}},&quot;matchColumnsResult&quot;:{&quot;isolateCache&quot;:{}}},&quot;mqConfigs&quot;:[{&quot;table&quot;:&quot;bbzobdaltestdb\\\\..*&quot;,&quot;topic&quot;:&quot;bbz.obtest.binlog&quot;,&quot;mqType&quot;:&quot;qmq&quot;,&quot;serialization&quot;:&quot;json&quot;,&quot;persistent&quot;:false,&quot;order&quot;:false,&quot;delayTime&quot;:0}],&quot;regex2Configs&quot;:{},&quot;regex2Filter&quot;:{},&quot;regex2Producers&quot;:{},&quot;tableName2Producers&quot;:{}}\"/>\n" +
                "         </dbCluster>\n" +
                "      </dbClusters>\n" +
                "   </dc>\n" +
                "</drc>";
    }

    @Test
    public void testTestAfterPropertiesSet() throws Exception {
        mqDelayMonitorServer.afterPropertiesSet();
    }

    @Test
    public void testGetAllMhaWithMessengerInLocalRegion() {
        Map<String, Set<Pair<String, String>>> map = mqDelayMonitorServer.getAllMhaWithMessengerInLocalRegion();
        Set<Pair<String, String>> allMhaWithMessengerInLocalRegion = map.get(MqType.qmq.name());
        Assert.assertEquals(1, allMhaWithMessengerInLocalRegion.size());
        Set<Pair<String, String>> allMhaWithMessengerInLocalRegionKafka = map.get(MqType.kafka.name());
        Assert.assertEquals(1, allMhaWithMessengerInLocalRegionKafka.size());
    }

    @Test
    public void testTestIsleader() throws Exception {
        mqDelayMonitorServer.isleader();
    }

    @Test
    public void testTestNotLeader() {
        mqDelayMonitorServer.notLeader();
    }

}