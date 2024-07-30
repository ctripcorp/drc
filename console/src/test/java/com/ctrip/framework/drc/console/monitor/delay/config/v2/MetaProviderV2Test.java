package com.ctrip.framework.drc.console.monitor.delay.config.v2;

import com.ctrip.framework.drc.console.monitor.delay.config.CompositeConfig;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.xml.sax.SAXException;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * @author: yongnian
 * @create: 2024/7/23 15:10
 */
public class MetaProviderV2Test {
    @Mock
    CompositeConfig compositeConfig;

    @InjectMocks
    MetaProviderV2 metaProviderV2;

    String drcXmlStrFromConsole = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
            "<drc>\n" +
            "    <dc id=\"shaoy\">\n" +
            "        <clusterManager ip=\"10.2.84.122\" port=\"8080\" master=\"true\"/>\n" +
            "        <zkServer address=\"10.2.84.112:2181\"/>\n" +
            "        <dbClusters>\n" +
            "            <dbCluster id=\"integration-test.fat-fx-drc1\" name=\"integration-test\" mhaName=\"fat-fx-drc1\" buName=\"BBZ\" appId=\"100024819\">\n" +
            "                <dbs readUser=\"m_drc_w_new\" readPassword=\"80+H44bA5wwqA(!R_new\" writeUser=\"m_drc_w_new\" writePassword=\"80+H44bA5wwqA(!R_new\" monitorUser=\"m_drc_w_new\" monitorPassword=\"80+H44bA5wwqA(!R_new\">\n" +
            "                    <db ip=\"10.2.72.233\" port=\"55222\" master=\"true\" uuid=\"12345678-a097-11ea-aade-fa163efb7175\"/>\n" +
            "                    <db ip=\"10.2.72.244\" port=\"55222\" master=\"false\" uuid=\"12345678-a098-11ea-a665-fa163eb5defa\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"10.2.87.155\" port=\"8088\" applierPort=\"8388\" gtidSkip=\"12345678-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
            "                <replicator ip=\"10.2.83.100\" port=\"8088\" applierPort=\"8388\" gtidSkip=\"12345678-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
            "                <applier ip=\"10.2.83.300\" port=\"8088\" targetIdc=\"sharb\" targetMhaName=\"fat-fx-drc2\" gtidExecuted=\"12345678-a097-11ea-aade-fa163efb7175:1-194\"/>\n" +
            "            </dbCluster>\n" +
            "            <dbCluster id=\"train.ticketorder\" name=\"train\" mhaName=\"ticketorder\" buName=\"TRA\" appId=\"100023456\">\n" +
            "                <dbs readUser=\"m_drc_w_new\" readPassword=\"80+H44bA5wwqA(!R_new\" writeUser=\"m_drc_w_new\" writePassword=\"80+H44bA5wwqA(!R_new\" monitorUser=\"m_drc_w_new\" monitorPassword=\"80+H44bA5wwqA(!R_new\">\n" +
            "                    <db ip=\"10.2.72.255\" port=\"55222\" master=\"true\" uuid=\"12345678-a097-11ea-aade-fa163efb7175\"/>\n" +
            "                    <db ip=\"10.2.72.266\" port=\"55222\" master=\"false\" uuid=\"12345678-a098-11ea-a665-fa163eb5defa\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"10.2.87.155\" port=\"8088\" applierPort=\"8389\" gtidSkip=\"12345678-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
            "                <replicator ip=\"10.2.83.100\" port=\"8088\" applierPort=\"8389\" gtidSkip=\"12345678-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
            "                <applier ip=\"10.2.83.300\" port=\"8089\" targetIdc=\"sharb\" targetMhaName=\"ticketorderrb\" gtidExecuted=\"12345678-a097-11ea-aade-fa163efb7175:1-194\"/>\n" +
            "            </dbCluster>\n" +
            "        </dbClusters>\n" +
            "    </dc>\n" +
            "    <dc id=\"sharb\">\n" +
            "        <clusterManager ip=\"10.2.84.109\" port=\"8080\" master=\"true\"/>\n" +
            "        <zkServer address=\"10.2.83.114:2181\"/>\n" +
            "        <dbClusters>\n" +
            "            <dbCluster id=\"integration-test.fat-fx-drc2\" name=\"integration-test\" mhaName=\"fat-fx-drc2\" buName=\"BBZ\" appId=\"100024819\">\n" +
            "                <dbs readUser=\"m_drc_w_new\" readPassword=\"80+H44bA5wwqA(!R_new\" writeUser=\"m_drc_w_new\" writePassword=\"80+H44bA5wwqA(!R_new\" monitorUser=\"m_drc_w_new\" monitorPassword=\"80+H44bA5wwqA(!R_new\">\n" +
            "                    <db ip=\"10.2.72.246\" port=\"55111\" master=\"true\" uuid=\"12345678-a099-11ea-a955-fa163ec687c3\"/>\n" +
            "                    <db ip=\"10.2.72.248\" port=\"55111\" master=\"false\" uuid=\"e434e42b-a09a-11ea-9340-fa163ebaf157\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"10.2.83.106\" port=\"8080\" applierPort=\"8384\" gtidSkip=\"69f3dff0-a098-11ea-a665-fa163eb5defa:1-194\"/>\n" +
            "                <applier ip=\"10.2.83.111\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"fat-fx-drc1\" gtidExecuted=\"63d32bf0-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
            "                <applier ip=\"10.2.86.136\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"fat-fx-drc1\" gtidExecuted=\"63d32bf0-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
            "            </dbCluster>\n" +
            "            <dbCluster id=\"train.ticketorderrb\" name=\"train\" mhaName=\"ticketorderrb\" buName=\"TRA\" appId=\"100023456\">\n" +
            "                <dbs readUser=\"m_drc_w_new\" readPassword=\"80+H44bA5wwqA(!R_new\" writeUser=\"m_drc_w_new\" writePassword=\"80+H44bA5wwqA(!R_new\" monitorUser=\"m_drc_w_new\" monitorPassword=\"80+H44bA5wwqA(!R_new\">\n" +
            "                    <db ip=\"10.2.72.256\" port=\"55111\" master=\"true\" uuid=\"12345678-a099-11ea-a955-fa163ec687c3\"/>\n" +
            "                    <db ip=\"10.2.72.258\" port=\"55111\" master=\"false\" uuid=\"e434e42b-a09a-11ea-9340-fa163ebaf157\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"10.2.83.106\" port=\"8080\" applierPort=\"8385\" gtidSkip=\"69f3dff0-a098-11ea-a665-fa163eb5defa:1-194\"/>\n" +
            "                <applier ip=\"10.2.83.111\" port=\"8088\" targetIdc=\"shaoy\" targetMhaName=\"ticketorder\" gtidExecuted=\"63d32bf0-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
            "                <applier ip=\"10.2.86.136\" port=\"8088\" targetIdc=\"shaoy\" targetMhaName=\"ticketorder\" gtidExecuted=\"63d32bf0-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
            "            </dbCluster>\n" +
            "        </dbClusters>\n" +
            "    </dc>\n" +
            "</drc>";

    public void init() throws IOException, SAXException {
//        Drc parse = DefaultSaxParser.parse(drcXmlStrFromConsole);
//        when(compositeConfig.getConfig()).thenReturn(drcXmlStrFromConsole);
//        when(compositeConfig.getDrc()).thenReturn(parse);
    }

    @Before
    public void setUp() throws IOException, SAXException {
        MockitoAnnotations.openMocks(this);
        init();
    }

    @Test
    public void testGetDrc() throws Exception {
        Drc parse = DefaultSaxParser.parse(drcXmlStrFromConsole);
        when(compositeConfig.getConfig()).thenReturn(drcXmlStrFromConsole);
        when(compositeConfig.getDrc()).thenReturn(parse);

        Drc result = metaProviderV2.getDrc();
        Assert.assertEquals(parse, result);
    }

    @Test
    public void testGetRealtimeDrc() throws Exception {
        Drc parse = DefaultSaxParser.parse(drcXmlStrFromConsole);
        when(compositeConfig.getConfig()).thenReturn(drcXmlStrFromConsole);
        when(compositeConfig.getDrc()).thenReturn(parse);

        Drc result = metaProviderV2.getRealtimeDrc();
        verify(compositeConfig, times(1)).updateConfig();
        Assert.assertEquals(parse, result);
    }

    @Test
    public void testGetDrcString() throws Exception {
        Drc parse = DefaultSaxParser.parse(drcXmlStrFromConsole);
        when(compositeConfig.getConfig()).thenReturn(drcXmlStrFromConsole);
        when(compositeConfig.getDrc()).thenReturn(parse);

        String result = metaProviderV2.getDrcString();
        Assert.assertEquals(drcXmlStrFromConsole, result);
    }

    @Test
    public void testGetRealtimeDrcString() throws Exception {
        Drc parse = DefaultSaxParser.parse(drcXmlStrFromConsole);
        when(compositeConfig.getConfig()).thenReturn(drcXmlStrFromConsole);
        when(compositeConfig.getDrc()).thenReturn(parse);

        String result = metaProviderV2.getRealtimeDrcString();
        verify(compositeConfig, times(1)).updateConfig();
        Assert.assertEquals(drcXmlStrFromConsole, result);
    }

    @Test
    public void testGetRealtimeDrcStringException() throws Exception {
        when(compositeConfig.getConfig()).thenReturn(null);
        when(compositeConfig.getDrc()).thenReturn(null);

        String result = metaProviderV2.getRealtimeDrcString();
        Assert.assertNull(result);
    }

}

