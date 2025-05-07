package com.ctrip.framework.drc.console.monitor.delay;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.mq.IKafkaDelayMessageConsumer;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerInfoDto;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2025/2/28 14:56
 */
public class KafkaDelayMonitorServerTest {

    @InjectMocks
    private KafkaDelayMonitorServer kafkaDelayMonitorServer;

    @Mock
    private DataCenterService dataCenterService;
    @Mock
    private MonitorTableSourceProvider monitorProvider;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MetaProviderV2 metaProviderV2;
    @Mock
    private ResourceService resourceService;
    @Mock
    private IKafkaDelayMessageConsumer kafkaConsumer;
    @Mock
    private CentralService centralService;

    private final static String localDc = "ntgxh";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(dataCenterService.getDc()).thenReturn(localDc);

        DcTbl dcTbl = new DcTbl();
        dcTbl.setId(1L);
        dcTbl.setDcName(localDc);
        Mockito.when(centralService.queryAllDcTbl()).thenReturn(Lists.newArrayList(dcTbl));

        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setMhaName("ob_zyn_test");
        mhaTblV2.setDcId(1L);

        MhaTblV2 mha1 = new MhaTblV2();
        mha1.setMhaName("test");
        mha1.setDcId(1L);

        Mockito.when(centralService.queryAllMhaTblV2()).thenReturn(Lists.newArrayList(mhaTblV2));

        Mockito.when(kafkaConsumer.resumeConsume()).thenReturn(true);
        Mockito.when(kafkaConsumer.stopConsume()).thenReturn(true);

        Mockito.doNothing().when(kafkaConsumer).mhasRefresh(Mockito.anyMap());
        Mockito.doNothing().when(kafkaConsumer).addMhas(Mockito.anyMap());
        Mockito.doNothing().when(kafkaConsumer).removeMhas(Mockito.anyMap());

        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setIp("127.0.0.1");
        resourceTbl.setDcId(1L);
        resourceTbl.setType(ModuleEnum.MESSENGER.getCode());

        Mockito.when(centralService.queryAllResourceTbl()).thenReturn(Lists.newArrayList(resourceTbl));
        Mockito.when(monitorProvider.getKafkaDelayMonitorSwitch()).thenReturn("on");
    }

    @Test
    public void testMonitorMessengerChange() throws Exception {

        Mockito.when(consoleConfig.getDcsInLocalRegion()).thenReturn(Sets.newHashSet(localDc));
        Mockito.when(metaProviderV2.getDrc()).thenReturn(buildDrc());

        MessengerInfoDto messengerInfoDto = new MessengerInfoDto();
        messengerInfoDto.setIp("127.0.0.1");
        messengerInfoDto.setRegistryKey("ob_zyn_test_dalcluster.ob_zyn_test._drc_kafka");
        Mockito.when(resourceService.getMasterMessengersInRegion(Mockito.anyString(), Mockito.anyList())).thenReturn(Lists.newArrayList(messengerInfoDto));

        Mockito.when(kafkaConsumer.resumeConsume()).thenReturn(true);
        kafkaDelayMonitorServer.isleader();
        Mockito.verify(centralService, Mockito.times(1)).queryAllMhaTblV2();

    }

    @Test
    public void testNotLeader() throws SQLException {
        kafkaDelayMonitorServer.notLeader();
        Mockito.verify(centralService, Mockito.never()).queryAllMhaTblV2();
    }

    @Test
    public void testSwitchListenMessenger() throws Exception {
        Map<String, String> map = Maps.newHashMap();
        map.put("ob_zyn_test", "127.0.0.1");
        map.put("test", "127.0.0.2");
        kafkaDelayMonitorServer.switchListenMessenger(map);
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
                "            <messenger ip=\"127.0.0.2\" port=\"8080\" gtidExecuted=\"3e0c9823-4da0-11ef-ad38-fa163eb2f6d2:1-11\" master=\"false\" applyMode=\"2\" nameFilter=\"bbzobdaltestdb\\..*\" properties=\"{&quot;nameFilter&quot;:&quot;bbzobdaltestdb\\\\..*&quot;,&quot;dataMediaConfig&quot;:{&quot;rowsFilters&quot;:[],&quot;ruleFactory&quot;:{},&quot;table2Config&quot;:{},&quot;table2ColumnConfig&quot;:{},&quot;table2Filter&quot;:{},&quot;table2ColumnFilter&quot;:{},&quot;table2Rule&quot;:{},&quot;table2ColumnRule&quot;:{},&quot;matchResult&quot;:{&quot;isolateCache&quot;:{}},&quot;matchColumnsResult&quot;:{&quot;isolateCache&quot;:{}}},&quot;mqConfigs&quot;:[{&quot;table&quot;:&quot;bbzobdaltestdb\\\\..*&quot;,&quot;topic&quot;:&quot;bbz.obtest.binlog&quot;,&quot;mqType&quot;:&quot;qmq&quot;,&quot;serialization&quot;:&quot;json&quot;,&quot;persistent&quot;:false,&quot;order&quot;:false,&quot;delayTime&quot;:0}],&quot;regex2Configs&quot;:{},&quot;regex2Filter&quot;:{},&quot;regex2Producers&quot;:{},&quot;tableName2Producers&quot;:{}}\"/>\n" +
                "            <messenger ip=\"127.0.0.1\" port=\"8080\" gtidExecuted=\"3e0c9823-4da0-11ef-ad38-fa163eb2f6d2:1-11\" master=\"false\" applyMode=\"5\" nameFilter=\"bbzobdaltestdb\\..*\" properties=\"{&quot;nameFilter&quot;:&quot;bbzobdaltestdb\\\\..*&quot;,&quot;dataMediaConfig&quot;:{&quot;rowsFilters&quot;:[],&quot;ruleFactory&quot;:{},&quot;table2Config&quot;:{},&quot;table2ColumnConfig&quot;:{},&quot;table2Filter&quot;:{},&quot;table2ColumnFilter&quot;:{},&quot;table2Rule&quot;:{},&quot;table2ColumnRule&quot;:{},&quot;matchResult&quot;:{&quot;isolateCache&quot;:{}},&quot;matchColumnsResult&quot;:{&quot;isolateCache&quot;:{}}},&quot;mqConfigs&quot;:[{&quot;table&quot;:&quot;bbzobdaltestdb\\\\..*&quot;,&quot;topic&quot;:&quot;bbz.obtest.binlog&quot;,&quot;mqType&quot;:&quot;qmq&quot;,&quot;serialization&quot;:&quot;json&quot;,&quot;persistent&quot;:false,&quot;order&quot;:false,&quot;delayTime&quot;:0}],&quot;regex2Configs&quot;:{},&quot;regex2Filter&quot;:{},&quot;regex2Producers&quot;:{},&quot;tableName2Producers&quot;:{}}\"/>\n" +
                "         </dbCluster>\n" +
                "      </dbClusters>\n" +
                "   </dc>\n" +
                "</drc>";
    }

    private Drc buildDrc() throws IOException, SAXException {
        return DefaultSaxParser.parse(getXml());
    }
}
