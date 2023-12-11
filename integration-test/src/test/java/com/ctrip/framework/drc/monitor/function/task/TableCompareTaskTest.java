package com.ctrip.framework.drc.monitor.function.task;

import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableInfo;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemaSnapshotTask;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dbs;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.google.common.collect.Lists;
import com.wix.mysql.distribution.Version;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.springframework.boot.test.mock.mockito.SpyBean;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.*;

public class TableCompareTaskTest {
    @InjectMocks
    TableCompareTask tableCompareTask;

    @Mock
    RegionConfig regionConfig;

    @SpyBean
    SchemaSnapshotTask schemaSnapshotTask;


    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetDbCluster() {
        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class);) {
            HashMap<String, String> map = new HashMap<>();
            String url = "http://mock.com";
            map.put("sha", url);
            when(regionConfig.getConsoleRegionUrls()).thenReturn(map);
            theMock.when(HttpUtils.get(Mockito.anyString(), Mockito.any()))
                    .thenReturn("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
                            "<drc>\n" +
                            "   <dc id=\"shaoy\" region=\"sha\">\n" +
                            "      <dbClusters>\n" +
                            "         <dbCluster id=\"test1_dalcluster.test1\" name=\"test1_dalcluster\" mhaName=\"test1\" buName=\"BBZ\" org-id=\"3\" appId=\"-1\" applyMode=\"1\">\n" +
                            "            <dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"root\">\n" +
                            "               <db ip=\"127.0.0.2\" port=\"3306\" master=\"true\" uuid=\"cd9a4a29-432f-11ee-baca-fa163e8041e7\"/>\n" +
                            "            </dbs>\n" +
                            "            <replicator ip=\"11.11.11.11\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"cd9a4a29-432f-11ee-baca-fa163e8041e7:1-2709736\" master=\"false\" excludedTables=\"\"/>\n" +
                            "         </dbCluster>\n" +
                            "      </dbClusters>\n" +
                            "   </dc>\n" +
                            "</drc>\n");
            System.out.println(tableCompareTask.getDbClusters());
        }
    }

    @Test
    public void testCompareMock() throws ExecutionException, InterruptedException {
        try (MockedStatic<TableCompareTask.SnapshotTaskForCompare> ioUtilsMockedStatic = Mockito.mockStatic(TableCompareTask.SnapshotTaskForCompare.class)) {

            Map<String, Map<String, String>> value = new HashMap<>();
            HashMap<String, String> tableMap = new HashMap<>();
            tableMap.put("benchmark2", "CREATE TABLE `bbzbbzdrcbenchmarktmpdb`.`benchmark2` (\n                                                        `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '空',\n                                                        `charlt256` char(30) DEFAULT NULL COMMENT '空',\n                                                        `chareq256` char(128) DEFAULT NULL COMMENT '空',\n                                                        `chargt256` char(255) DEFAULT NULL COMMENT '空',\n                                                        `varcharlt256` varchar(30) DEFAULT NULL COMMENT '空',\n                                                        `varchareq256` varchar(256) DEFAULT NULL COMMENT '空',\n                                                        `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '空',\n                                                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n                                                        `drc_id_int` int(11) NOT NULL DEFAULT '1' COMMENT '空',\n                                                        `addcol1` varchar(64) DEFAULT 'default_addcol1' COMMENT 'test',\n                                                        `addcol2` varchar(64) DEFAULT 'default_addcol2' COMMENT 'test',\n                                                        `drc_char_test_2` char(30) DEFAULT 'char' COMMENT '空',\n                                                        `drc_tinyint_test_2` tinyint(4) DEFAULT '12' COMMENT '空',\n                                                        `drc_bigint_test` bigint(20) DEFAULT '120' COMMENT '空',\n                                                        `drc_integer_test` int(11) DEFAULT '11' COMMENT '空',\n                                                        `drc_mediumint_test` mediumint(9) DEFAULT '12345' COMMENT '空',\n                                                        `drc_time6_test` time DEFAULT '02:02:02' COMMENT '空',\n                                                        `drc_datetime3_test` datetime(3) DEFAULT '2019-01-01 01:01:01.000' COMMENT '空',\n                                                        `drc_year_test` year(4) DEFAULT '2020' COMMENT '空',\n                                                        `hourly_rate_3` decimal(10,2) NOT NULL DEFAULT '1.00' COMMENT '空',\n                                                        `drc_numeric10_4_test` decimal(10,4) DEFAULT '100.0000' COMMENT '空',\n                                                        `drc_float_test` float DEFAULT '12' COMMENT '空',\n                                                        `drc_double_test` double DEFAULT '123' COMMENT '空',\n                                                        `drc_bit4_test` bit(1) DEFAULT b'1' COMMENT 'TEST',\n                                                        `drc_double10_4_test` double(10,4) DEFAULT '123.1245' COMMENT '空',\n                                                        `drc_real_test` double DEFAULT '234' COMMENT '空',\n                                                        `drc_real10_4_test` double(10,4) DEFAULT '23.4000' COMMENT '空',\n                                                        `drc_binary200_test_2` binary(200) DEFAULT 'binary2002\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0' COMMENT '空',\n                                                        `drc_varbinary1800_test_2` varbinary(1800) DEFAULT 'varbinary1800' COMMENT '空',\n                                                        `addcol` varchar(50) DEFAULT 'addColName' COMMENT '添加普通Name',\n                                                        PRIMARY KEY (`id`),\n                                                        KEY `ix_DataChange_LastTime` (`datachange_lasttime`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='test';");
            value.put("bbzbbzdrcbenchmarktmpdb", tableMap);
            ioUtilsMockedStatic.when(() -> TableCompareTask.SnapshotTaskForCompare.doSnapshot(any(), any()))
                    .thenReturn(value);

            TableCompareTask.MySQLWrapper mysql5 = mock(TableCompareTask.MySQLWrapper.class);
            TableCompareTask.MySQLWrapper mysql8 = mock(TableCompareTask.MySQLWrapper.class);
            TableInfo value1 = new TableInfo();
            value1.setDbName("test");
            value1.setTableName("test");
            value1.setColumnList(Lists.newArrayList(new TableMapLogEvent.Column("col1", false, "char",
                    "1", "s2", "s3",
                    "s4", "s5", "s6", "s7",
                    "s8", "s9", "columnDefaultValue")));
            value1.setIdentifiers(Lists.newArrayList());
            when(mysql8.find(anyString(), anyString())).thenReturn(value1);
            when(mysql5.find(anyString(), anyString())).thenReturn(value1);
            ArrayList<DbCluster> mhaList = Lists.newArrayList(new DbCluster().setMhaName("test1").setDbs(new Dbs().addDb(new Db().setMaster(true).setIp("1.0.0.1").setPort(3306))));
            TableCompareTask.Result compare = tableCompareTask.compare(mysql5, mysql8, mhaList);
            Assert.assertTrue(compare.allSame());
        }
    }

//    @Test
    public void testCompareRealReal() throws ExecutionException, InterruptedException {
        try (TableCompareTask.MySQLWrapper mysql8 = new TableCompareTask.MySQLWrapper(10008, Version.v8_0_32);
             TableCompareTask.MySQLWrapper mysql5 = new TableCompareTask.MySQLWrapper(10007, Version.v5_7_23);) {

            List<String> mhaList = Lists.newArrayList("sinsyspub");
            List<DbCluster> collect = mhaList.stream().map(e -> new DbCluster().setMhaName(e).setDbs(new Dbs().addDb(new Db().setMaster(true).setIp("1.0.0.1").setPort(3306)))).collect(Collectors.toList());
            TableCompareTask.Result compare = tableCompareTask.compare(
                    mysql8,
                    mysql5,
                    collect
            );
            Assert.assertTrue(compare.allSame());
        }
    }


    public void listAllMhas() {
        List<String> strings = listFilesUsingJavaIO();
        List<String> exlude = listExclude();
        System.out.println(String.join(",", strings));
        strings.removeAll(exlude);
        System.out.println(strings.size());
        System.out.println(strings);
    }

    public List<String> listExclude() {
        String s = "htlhotelos7,trntripcntrain,bbzcommonbizshard09,bbzappcrashusershard07new,bbzappcrashusershard04new,bbzappcrashusershard03cls,bbzappcrashusernew,fltreimbursementnew,bbzmobileserviceshard,fltrebook,fltorderflow,fltcheckin,flttriprecordshard16,bbzimdetailshard01,bbzimdetailshard02,bbzimdetailshard03,bbzimdetailshard04,bbzimdetailshard05,bbzimdetailshard06,bbzimdetailshard07,bbzimdetailshard08,gspubs01,fltffpcardshard01,ccardopenplatformnew,ccardvccgroup01,bbzinspub03,htlbiseqrules,ttdbookingsshard0,ttdbookingsshard4,ttdorder,ttdorderoffline,ttdorderschedule,tourttdprocpub01,ibuflight7new,bbzim,ccardvccgroup02,htlroomnew,hltprofile,htlroompricebaknew,htldataadsortnew,htlindividualnew,htlproductmatch,htlofflineanalysis,htlovsroomsinventorymshard101,htlovsroomsinventorymshard102,htlovsroomsinventorymshard103,htlovsroomsinventorymshard104,htlpricinganalysis,htlovsroomsinventorymshard201,htlovsroomsinventorymshard202,htlovsroomsinventorymshard203,htlovsroomsinventorymshard204,htlroomsinventorynew,fltfullskillos7new,htlpriceserviceconfig,htlinputmsgqueue,htlinfogovern,htlpricinginfo,userrecommendationos7,htlalliance,htlovslimitinfoshard1,htlovslimitinfoshard2,mktpromobasic,fncbillingos7,fncaccountpub02,fltsettlownagencyshard0,fltsettlownagencyshard16,fltsettlownagencyos7new,fltsettlementprocessos7new,mktpromocodeshard01,mktpromocodeshard09,mktpromocodeshard17,mktpromocodeshard25,rskbwlistmanage,pkgcommentos7new,ttdcommunity,frtshoppingcart,fltordershardmulti0403,fltordershardmulti0404,fltordershardmulti0803,fltordershardmulti0804,fltordershardmulti1201,fltordershardmulti1202,fltordershardmulti1203,fltordershardmulti1204,fncccauthpub02,groupckvos7new,ttdsettlementnew,fltordershardmulti2801,fltordershardmulti3204,fltordershardmulti2802,fltordershardmulti2803,fltordershardmulti3201,fltordershardmulti2804,fltordershardmulti3202,fltordershardmulti3203,fltordershardmulti3601,fltordershardmulti3602,fltordershardmulti3603,fltordershardmulti5601,fltordershardmulti5602,fltordershardmulti5603,fltordershardmulti5604,fltordershardmulti6001,fltordershardmulti6002,fltordershardmulti6003,fltordershardmulti6004,fltordershardmulti4801,fltordershardmulti4802,fltordershardmulti4803,fltordershardmulti4804,fltordershardmulti5201,fltordershardmulti5202,ibupub3group02,fncpaymentgatewayos7new,fltchangeorderos7new,ticketingtpplication,fncpaymentcbuorder01,fncpaymentcbuorder01new,fncpaypub4groupnew04,fcsworkorder,fltinsclaim,gscommentnew,fxdalclusterbenchmark,tourordercentralshard05,tourvacationordershard,tourpassproduct,fncpaymentgatewayos8,fncibutrade,mktchannel,fncuserpub01,fncusersecurityshardnew,bbzbigdataportalnew,corpreport,fncmiddlepaypayingshard56new,fncmiddlepaypayingshard48,fncmiddlepaypayingshard41new,fncmiddlepaypayingshard2new,fncmiddlepaypayingshard25new,fncmiddlepaypayingshard16,fncmiddlepaypayingshard9new,fncmiddlepaypayingshardnew,gssimplepoi,fltchangeflight,fncbcauthnew,fncpaymentcommonauthos7,tourorder,pkgmyorder,fncpaymentpub01group02,fncccauthpub01,fncpaypub4group01,fncctripuserverify05,fncccauthmanageshard06,fncccauthmanageshard07,fxdrctmpshali,bbzimelongshali,bbzimelongdetailshardshali,frabbzpubfraaws,fraibupubfraaws,fratestpub,fratrntripcntrain,fratrnpub,frafltpub04,fraflttravixpub02,fraflttravixpub01,frafltonexpub01,frabbzcredentialshard,frabbzmembersaccountshard,frabbzpub,fraibupub01,fracarpub01,frabbzcommon,frafltpub,commonorderconfig,commonordershard2os7,commonordershard5os7,commonordershard3os7,commonordershard8os7,commonordershard9os7,commonordershard11os7,commonordershard12os7,commonordershard6os7,commonordershard1oy,commonordershard4oy,commonordershard7oy,commonordershard10oy,carsdvbknew,commonorderarchivedshard1oy,commonorderarchivedshard7oy,bbzimelongdetailshard,bbzimelongdetailshard03os7,bbzimelongdetailshard07os7,bbzimelong,bbzimelongdetailshard5,commonorderarchivedshard4shaxy,commonorderarchivedshard10shaxy,itcchatos7,ibusearchos7new,htlmobile,bbzappcrashbriefos7new,trnglobalrailgdsdicnew,trnintlorder,htlsearchsrnew,gsuserm,gspubs04,fltresource,fltintlguardpay,fltintlcache,fltintlaggbusiness,ibucommon,ibucontent,fltresourcetranslation,ibupub1group02,ibupub1group01,bbzimelongdetailshardgroup,bbzimelongdetailshard3group,bbzimelongdetailshard5group,bbzimelongdetailshard7group,bbzmembersaccountshard01,bbzmembersaccountshard09,bbzaccountsshard01os7new,bbzaccountsshard09os7new,bbzcredentialsshard1,bbzcredentialsshard5,bbzcredentialsshard9,bbzcredentialsshard13,fltintlresourceoag,publictest03,bbzgeolocation,fltintlresourceetl,ibupub2group01,fltbidatagroup02,bbzmbrcomminfo,bbzmbrcomminfoshard02,bbzmbrcommoncontactshard01,bbzmbrcommoncontactshard02,bbzmembersinfonew,bbzmembersinfo2new,bbzdatacell04,bbzdatacell01,htlcommnew,bbzbrowsehistoryshard01,bbzbrowsehistoryshard09,bbzrtuserbehavior,bbzbrowsehistoryshard25,bbzrtuserbehaviorshard03,bbzbrowsehistoryshard41,bbzrtuserbehaviorshardnew04,bbzbrowsehistoryshard57,bbzproductindex,fltorderprocesspub02,ibupub2group03,ibuorderindexnew,corpaggfltpolicyos7,ibucommission,fltrefundos7new,ibupub1group04,ttdvbkordernew,ibumarketmsg,fltorderprocesspub01,flttriprecordshardnew,dpdatastreamingnew,ibupub3group01,fltsettlcoopflightagencyshard0,fltxapiconfig,flttradepolicymos7new,fltdomcacheos7,gscrawlerbigdatanew,trnintlproduct,ibupub1group03,bbzinspub04,fltdombookingos7new,fltintlbookinglog,carchproduct,dcsproductm,igtgeocls,fltintlvacation,fncgcaccountnew,gshotsale,globalsearchnew,igtbinew,ttlorderbooking,fltnotifyorderindexnew,ttdbookingsshardv01,ttdbookingsshardv02,carisd,centralloggingnew,carspubnew,carsettlement,carmetadatanew,fltchangemonitor,aeskeyinternal,fltorderlogv1,fltordprocessviewshard32,fltexchangeshard01,carorderbakos7,ottdproduct,ottdglobalpub0101,pkgproductmetanew,ttdpoinew,ttdproc01,fltticketissueos7new,tourproduct,htlgroupproduct,fltintlpolicyshard00new,bbzappcrashbrief,softipt,fncpaymentuserrelationshard01,bizsmsqmq,bbzsyspackage,frtshoppingorder,fltordershardmulti0001,fltordershardmulti0002,fltordershardmulti0003,fltordershardmulti0004,fltordershardmulti0401,fltordershardmulti0402,fltordershardmulti0801,fltordershardmulti0802,fltordershardmulti1601,fltordershardmulti1602,fltordershardmulti1603,fltordershardmulti1604,fltordershardmulti2001,fltordershardmulti2002,fltordershardmulti2003,fltordershardmulti2004,fltordershardmulti2401,fltordershardmulti2402,fltordershardmulti2403,fltordershardmulti2404,fltordershardmulti3604,fltordershardmulti4001,fltordershardmulti4002,fltordershardmulti4003,fltordershardmulti4004,fltordershardmulti4401,fltordershardmulti4402,fltordershardmulti4403,fltordershardmulti4404,ccsoftexchangev1,fltordershardmulti5203,fltordershardmulti5204,fncprotocolmanageprocessshard01,fnctpauthpaymentcloudpub01,ccopensips,splitestalma01,fncpaymentgatewayos7,fnccheetahshard06new,fncccauthmanageshard01,fnctpauthofflineshard09new,fnctpauthofflineshard01new,fncauthmergemanage01,sinibupub2,sinbbzdrctest,sinbbzmemberpub,sinfltpub01,sinsyspub,sincommonordershard,sinbbzcredentialshard,sinbbzmembersinfoshard,sincommonorderarchivedshard,sincommonordershard5,sincommonordershard9,sinhtlpub01,sinbbzbrowsehistoryshard,sinbbzbrowsehistoryshard2,sinbbzcommonbizshard,sinttdvbkpub,sintrnpub01,sinbbzimcorepub,sinccardvccpub,sinhtlpub02,sinttdorder,sinttdorderprocess,sinhtlpub03,sinmktpub01,sinfncpub01,sinfltpub02,sinfltpub03,singstrippub01,sinmktpub02,sinfltpub04,sincarpub01,sinriskvariableshard,sinttlpub01,sinibupub3,sinfltpub05,sintrninc7,sinbdaipub01,sinibumarketingaccess,sinibuppcplatform,sinibufeedmanage,sinhtlseopub,sinfltordershard4,sinfltordershard8,sinfltordershard16,sinfltordershard24,sinfltordershard32,sinfltordershard40,sinfltordershard48,sinfltordershard60,sinfncpaymentgateway,sintourorderpub,sinibu80pub01,sinbbzconfig,sinfncpaymentcbuordershard,sinfnctradeordershard,sinfncpaymentgatewayshard,sinfncpaypayingshard01,sincorppub01,sinfncpaypayingshard02,fncplaincardordershard01,gsdestm,sinibupub,fncplaincardordershard01os7,sinfncrefundcenter,pkgmyorderlogos7new,ibusearchos7fraaws,fncpaymentgateway02,fncplaincardordershard13new,frafltdrcpub01,gsbigdatabusinessnew,sinmktpub03,sinibumobile,fncrouterfrontcashier,sinfncauthpub02,ccardvccgroup04,sinibu80pub02,sinfncauthpub01";
        return Arrays.stream(s.split(",")).collect(Collectors.toList());
    }

    public List<String> listFilesUsingJavaIO() {
        return Stream.of(new File("/opt/data/drc/schemas").listFiles())
                .filter(file -> !file.isDirectory())
                .map(File::getName)
                .map(e -> e.split("\\.")[0])
                .collect(Collectors.toList());
    }

    @Test
    public void testGetSaveData() throws IOException {
        TableCompareTask tableCompareTask1 = new TableCompareTask();

        HashMap<String, Map<String, String>> saveData = new HashMap<>();
        HashMap<String, String> tableMap = new HashMap<>();
        tableMap.put("table", "create table balabala");
        saveData.put("db", tableMap);
        String mhaName = "testMha";
        tableCompareTask1.saveToLocalData(mhaName, saveData);

        Map<String, Map<String, String>> data = tableCompareTask1.tryGetLocalData(mhaName);
        Assert.assertEquals(data, saveData);
    }


    @Test
    public void testCompareDb() {
        Assert.assertTrue(TableCompareTask.isDbSame("db1", "db1", new StringBuilder()));
        Assert.assertFalse(TableCompareTask.isDbSame("db1", "db2", new StringBuilder()));
        Assert.assertFalse(TableCompareTask.isDbSame(null, "db2", new StringBuilder()));

    }

    @Test
    public void testCompareTable() {
        Assert.assertTrue(TableCompareTask.isTableSame("table1", "table1", new StringBuilder()));
        Assert.assertFalse(TableCompareTask.isTableSame("table1", "table2", new StringBuilder()));
        Assert.assertFalse(TableCompareTask.isTableSame(null, "table2", new StringBuilder()));
    }

    @Test
    public void testCompareColumns() {

        List<TableMapLogEvent.Column> empty = new ArrayList<>();
        List<TableMapLogEvent.Column> one = new ArrayList<>();
        one.add(new TableMapLogEvent.Column());
        Assert.assertFalse(TableCompareTask.isColumnListSame(empty, one, new StringBuilder()));

        List<TableMapLogEvent.Column> columnList8 = new ArrayList<>();
        List<TableMapLogEvent.Column> columnList5 = new ArrayList<>();
        Assert.assertTrue(TableCompareTask.isColumnListSame(columnList8, columnList5, new StringBuilder()));

        TableMapLogEvent.Column c1 = new TableMapLogEvent.Column("col1", false, "char",
                "1", "s2", "s3",
                "s4", "s5", "s6", "s7",
                "s8", "s9", "columnDefaultValue");
        TableMapLogEvent.Column c2 = new TableMapLogEvent.Column("col1", false, "char",
                "1", "s2", "s3",
                "s4", "s5", "s6", "s7",
                "s8", "s9", "columnDefaultValue");
        columnList8.add(c1);
        columnList5.add(c2);
        Assert.assertTrue(TableCompareTask.isColumnListSame(columnList8, columnList5, new StringBuilder()));

        TableMapLogEvent.Column c3 = new TableMapLogEvent.Column("col1", false, "char",
                "1", "s2", "s3",
                "s4", "s5", "s6", "s7",
                "s8", "s9", "columnDefaultValue");
        TableMapLogEvent.Column c4 = new TableMapLogEvent.Column("col1", false, "char",
                "1", "s2", "s3",
                "s4", "s5", "s6", "s7",
                "s8", "s9", "notEquals");
        columnList8.add(c3);
        columnList5.add(c4);
        Assert.assertFalse(TableCompareTask.isColumnListSame(columnList8, columnList5, new StringBuilder()));

    }

    @Test
    public void compareIdentifiers() {
        List<List<String>> identifiers5 = new ArrayList<>();
        identifiers5.add(Lists.newArrayList("pk1"));
        identifiers5.add(Lists.newArrayList("uk1"));
        identifiers5.add(Lists.newArrayList("uk2"));


        List<List<String>> identifiers8 = new ArrayList<>();
        identifiers8.add(Lists.newArrayList("pk1"));
        identifiers8.add(Lists.newArrayList("uk2"));
        identifiers8.add(Lists.newArrayList("uk1"));
        StringBuilder sb = new StringBuilder();
        boolean identifiersSame = TableCompareTask.isIdentifiersSame(identifiers8, identifiers5, sb);
        Assert.assertTrue(identifiersSame);
        identifiers8.get(2).set(0, "uk2");
        identifiersSame = TableCompareTask.isIdentifiersSame(identifiers8, identifiers5, sb);
        Assert.assertFalse(identifiersSame);
    }
}
