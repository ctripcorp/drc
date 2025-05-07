package com.ctrip.framework.drc.console.service.remote.qconfig;


import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.remote.qconfig.request.CreateFileRequestBody;
import com.ctrip.framework.drc.console.service.remote.qconfig.request.UpdateRequestBody;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.BatchUpdateResponse;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.CreateFileResponse;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.FileDetailData;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.FileDetailResponse;
import com.ctrip.framework.drc.console.utils.MySqlUtils.TableSchemaName;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.sql.SQLException;
import java.util.*;

public class QConfigServiceImplTest {

    @InjectMocks
    private QConfigServiceImpl qConfigService;

    @Mock
    private DomainConfig domainConfig;

    @Mock
    private DbClusterApiService dbClusterService;

    @Mock
    private EventMonitor eventMonitor;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.doReturn("db1_dalcluster").when(dbClusterService).getDalClusterName(Mockito.anyString(), Mockito.anyString());
        Mockito.doNothing().when(eventMonitor).logEvent(Mockito.anyString(), Mockito.anyString());
        Mockito.when(domainConfig.getDalClusterUrl()).thenReturn("dalclusterUrl");
        Mockito.when(domainConfig.getQConfigRestApiUrl()).thenReturn("url");
        Mockito.when(domainConfig.getQConfigAPIToken()).thenReturn("token");
        Mockito.when(domainConfig.getDc2QConfigSubEnvMap()).thenReturn(new HashMap<>() {{
            put("sinaws", "SIN-AWS");
            put("shaxy", "SHAXY");
        }});
        Mockito.when(domainConfig.getIDCsInSameRegion("shaxy")).thenReturn(Sets.newHashSet("shaxy"));
    }

    @Test
    public void testAddOrUpdateDalClusterMqConfig() throws SQLException {
        String envName = Foundation.server().getEnv().getName().toLowerCase();
        // create file
        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> {
                HttpUtils.get(
                        Mockito.eq("url/configs"
                                + "?token={token}"
                                + "&groupid={groupid}"
                                + "&dataid={dataid}"
                                + "&env={env}"
                                + "&subenv={subenv}"
                                + "&targetgroupid={targetgroupid}"),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(JsonUtils.toJson(mockInExistentFileDetailResponse()));
            theMock.when(() -> {
                HttpUtils.post(
                        Mockito.eq("url" + "/configs" + "?token={token}"),
                        Mockito.any(CreateFileRequestBody.class),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(mockCreateFileResponse());
            boolean res = qConfigService.addOrUpdateDalClusterMqConfig(
                    "shaxy",
                    "topicName",
                    "db1\\.t3",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t3"));
                    }}
            );
            Assert.assertTrue(res);
        }


        // update file
        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> {
                HttpUtils.get(
                        Mockito.eq("url/configs"
                                + "?token={token}"
                                + "&groupid={groupid}"
                                + "&dataid={dataid}"
                                + "&env={env}"
                                + "&subenv={subenv}"
                                + "&targetgroupid={targetgroupid}"),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(JsonUtils.toJson(mockExistingFileDetailResponse()));

            theMock.when(() -> {
                HttpUtils.post(Mockito.eq("url"
                                + "/properties/binlog-topic-registry/envs/"
                                + envName
                                + "/subenvs/SHAXY"
                                + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}"),
                        Mockito.anyString(),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(mockBatchUpdateResponse());
            boolean res = qConfigService.addOrUpdateDalClusterMqConfig(
                    "shaxy",
                    "topicName",
                    "db1\\.t3",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t3"));
                    }}
            );
            Assert.assertTrue(res);
        }
    }

    private static CreateFileResponse mockCreateFileResponse() {
        return new CreateFileResponse() {{
            setStatus(0);
        }};
    }


    @Test
    public void testDisableDalClusterMqConfigIfNecessary() throws SQLException {
        String envName = Foundation.server().getEnv().getName().toLowerCase();
        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> {
                HttpUtils.get(
                        Mockito.eq("url/configs"
                                + "?token={token}"
                                + "&groupid={groupid}"
                                + "&dataid={dataid}"
                                + "&env={env}"
                                + "&subenv={subenv}"
                                + "&targetgroupid={targetgroupid}"),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(JsonUtils.toJson(mockExistingFileDetailResponse()));


            theMock.when(() -> {
                HttpUtils.post(Mockito.eq("url"
                                + "/properties/binlog-topic-registry/envs/"
                                + envName
                                + "/subenvs/SHAXY"
                                + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}"),
                        Mockito.anyString(),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(mockBatchUpdateResponse());
            boolean b = qConfigService.removeDalClusterMqConfigIfNecessary(
                    "shaxy",
                    "topicName",
                    "db1\\.t1",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t1"));
                    }},
                    null
            );

            boolean b1 = qConfigService.removeDalClusterMqConfigIfNecessary(
                    "shaxy",
                    "topicName",
                    "db1\\.t1",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t1"));
                    }},
                    Lists.newArrayList("db1\\.t3")
            );

            boolean b2 = qConfigService.removeDalClusterMqConfigIfNecessary(
                    "shaxy",
                    "topicName",
                    "db1\\.t1",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t1"));
                    }},
                    Lists.newArrayList("db1\\..*")
            );
            Assert.assertTrue(b & b1 & b2);
        }
    }


    @Test
    public void testUpdate() throws SQLException {
        String envName = Foundation.server().getEnv().getName().toLowerCase();
        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> {
                HttpUtils.get(
                        Mockito.eq("url/configs"
                                + "?token={token}"
                                + "&groupid={groupid}"
                                + "&dataid={dataid}"
                                + "&env={env}"
                                + "&subenv={subenv}"
                                + "&targetgroupid={targetgroupid}"),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(JsonUtils.toJson(mockExistingFileDetailResponse()));


            theMock.when(() -> {
                HttpUtils.post(Mockito.eq("url"
                                + "/properties/binlog-topic-registry/envs/"
                                + envName
                                + "/subenvs/SHAXY"
                                + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}"),
                        Mockito.anyString(),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(mockBatchUpdateResponse());

            boolean b1 = qConfigService.updateDalClusterMqConfig(
                    "shaxy",
                    "topicName",
                    "dalclusterName",
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t1"));
                    }}
            );

            boolean b2 = qConfigService.updateDalClusterMqConfig(
                    "shaxy",
                    "topicName",
                    "dalclusterName",
                    new ArrayList<>() {{
                        add(new TableSchemaName("db2", "t2"));
                    }}
            );
            Assert.assertTrue(b1 & b2);
        }
    }

    @Test
    public void testReWriteUpdateDalClusterMqConfig() throws SQLException {
        String envName = Foundation.server().getEnv().getName().toLowerCase();
        // test update
        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> {
                HttpUtils.get(
                        Mockito.eq("url/configs"
                                + "?token={token}"
                                + "&groupid={groupid}"
                                + "&dataid={dataid}"
                                + "&env={env}"
                                + "&subenv={subenv}"
                                + "&targetgroupid={targetgroupid}"),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(JsonUtils.toJson(mockExistingFileDetailResponse()));


            theMock.when(() -> {
                HttpUtils.post(Mockito.eq("url"
                                + "/properties/binlog-topic-registry/envs/"
                                + envName
                                + "/subenvs/SHAXY"
                                + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}"),
                        Mockito.anyString(),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(mockBatchUpdateResponse());


            LinkedHashMap<String, String> configContext = new LinkedHashMap<>();
            configContext.put("bbz.test.binlog_1.status", "on");
            configContext.put("bbz.test.binlog_1.dbName", "db1");
            configContext.put("bbz.test.binlog_1.tableName", "table1");
            boolean updateResult = qConfigService.reWriteDalClusterMqConfig(
                    "shaxy",
                    "db1_dalcluster",
                    configContext
            );
            Assert.assertTrue(updateResult);

            theMock.verify(
                    Mockito.times(1),
                    () -> HttpUtils.post(Mockito.anyString(), Mockito.argThat(jsonString -> {
                        List<UpdateRequestBody> requests = JsonUtils.fromJsonToList((String) jsonString, UpdateRequestBody.class);
                        UpdateRequestBody updateRequestBody = requests.get(0);
                        return updateRequestBody.getData().equals(configContext);
                    }), Mockito.eq(BatchUpdateResponse.class), Mockito.anyMap())
            );
        }
    }

    @Test
    public void testReWriteCreateDalClusterMqConfig() throws SQLException {
        String envName = Foundation.server().getEnv().getName().toLowerCase();
        // test update
        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> {
                HttpUtils.get(
                        Mockito.eq("url/configs"
                                + "?token={token}"
                                + "&groupid={groupid}"
                                + "&dataid={dataid}"
                                + "&env={env}"
                                + "&subenv={subenv}"
                                + "&targetgroupid={targetgroupid}"),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(JsonUtils.toJson(mockInExistentFileDetailResponse()));


            theMock.when(() -> {
                HttpUtils.post(Mockito.eq("url/configs?token={token}"),
                        Mockito.any(CreateFileRequestBody.class),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(mockCreateFileResponse());


            LinkedHashMap<String, String> configContext = new LinkedHashMap<>();
            configContext.put("bbz.test.binlog_1.status", "on");
            configContext.put("bbz.test.binlog_1.dbName", "db1");
            configContext.put("bbz.test.binlog_1.tableName", "table1");
            boolean updateResult = qConfigService.reWriteDalClusterMqConfig(
                    "shaxy",
                    "db1_dalcluster",
                    configContext
            );
            Assert.assertTrue(updateResult);

            theMock.verify(
                    Mockito.times(1),
                    () -> HttpUtils.post(Mockito.anyString(), Mockito.argThat(requestBody -> {
                        CreateFileRequestBody requestBody1 = (CreateFileRequestBody) requestBody;
                        String content = (String) requestBody1.getConfig().get("content");
                        Map<String, String> config = qConfigService.string2config(content);
                        return config.equals(configContext);
                    }), Mockito.eq(CreateFileResponse.class), Mockito.anyMap())
            );
        }
    }


    @Test
    public void testProcessRemovedTopic() {
        LinkedHashMap<String, String> configContext = new LinkedHashMap<>();
        configContext.put("bbz.test.binlog_1.status", "on");
        configContext.put("bbz.test.binlog_1.dbName", "db1");
        configContext.put("bbz.test.binlog_1.tableName", "table1");
        LinkedHashMap<String, String> targetConfig = new LinkedHashMap<>(configContext);

        // origin empty, no need to append
        HashMap<String, String> originalConfig = new HashMap<>();
        QConfigServiceImpl.processRemovedTopic(configContext, originalConfig);
        Assert.assertEquals(configContext, targetConfig);

        // origin all exist, non eed to append
        originalConfig = new HashMap<>();
        originalConfig.put("bbz.test.binlog_1.status", "on");
        originalConfig.put("bbz.test.binlog_1.dbName", "db3");
        originalConfig.put("bbz.test.binlog_1.tableName", "table2");
        QConfigServiceImpl.processRemovedTopic(configContext, originalConfig);
        Assert.assertEquals(configContext, targetConfig);

        // origin removed, need to append
        originalConfig = new HashMap<>();
        originalConfig.put("bbz.test.binlog_2.status", "on");
        originalConfig.put("bbz.test.binlog_2.dbName", "db3");
        originalConfig.put("bbz.test.binlog_2.tableName", "table2");
        QConfigServiceImpl.processRemovedTopic(configContext, originalConfig);
        targetConfig = new LinkedHashMap<>(configContext);
        targetConfig.put("bbz.test.binlog_2.status", "off");
        targetConfig.put("bbz.test.binlog_2.dbName", "");
        targetConfig.put("bbz.test.binlog_2.tableName", "");

        Assert.assertEquals(configContext, targetConfig);
    }

    @Test
    public void testSameTableMultiTopic() throws SQLException {
        String envName = Foundation.server().getEnv().getName().toLowerCase();
        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> {
                HttpUtils.get(
                        Mockito.eq("url/configs"
                                + "?token={token}"
                                + "&groupid={groupid}"
                                + "&dataid={dataid}"
                                + "&env={env}"
                                + "&subenv={subenv}"
                                + "&targetgroupid={targetgroupid}"),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(JsonUtils.toJson(mockExistingQmDetailResponse()));


            theMock.when(() -> {
                HttpUtils.post(Mockito.eq("url"
                                + "/properties/binlog-topic-registry/envs/"
                                + envName
                                + "/subenvs/SHAXY"
                                + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}"),
                        Mockito.anyString(),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(mockBatchUpdateResponse());

            boolean b1 = qConfigService.addOrUpdateDalClusterMqConfig(
                    "shaxy",
                    "topicName2",
                    "dalclusterName",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t1"));
                    }}
            );
            theMock.verify(Mockito.times(0), () -> HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.eq(BatchUpdateResponse.class), Mockito.anyMap()));

            boolean b2 = qConfigService.updateDalClusterMqConfig(
                    "shaxy",
                    "topicName2",
                    "dalclusterName",
                    new ArrayList<>() {{
                        add(new TableSchemaName("db2", "t2"));
                    }}
            );
            theMock.verify(Mockito.times(0), () -> HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.eq(BatchUpdateResponse.class), Mockito.anyMap()));

            boolean b3 = qConfigService.addOrUpdateDalClusterMqConfig(
                    "shaxy",
                    "topicName2",
                    "dalclusterName",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t1"));
                        add(new TableSchemaName("db2", "t3"));
                    }}
            );
            theMock.verify(Mockito.times(1), () -> HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.eq(BatchUpdateResponse.class), Mockito.anyMap()));

        }
    }


    private BatchUpdateResponse mockBatchUpdateResponse() {
        BatchUpdateResponse batchUpdateResponse = new BatchUpdateResponse();
        batchUpdateResponse.setStatus(0);
        return batchUpdateResponse;
    }

    private FileDetailResponse mockInExistentFileDetailResponse() {
        FileDetailResponse fileDetail = new FileDetailResponse();
        fileDetail.setStatus(-1);
        return fileDetail;
    }

    private FileDetailResponse mockExistingFileDetailResponse() {
        FileDetailResponse fileDetail = new FileDetailResponse();
        fileDetail.setStatus(0);
        fileDetail.setMessage("message");

        FileDetailData fileDetailData = new FileDetailData();
        fileDetailData.setEditVersion(0);
        fileDetailData.setData("topicName.status=off\ntopicName.dbName=db1,db2\ntopicName.tableName=t1,t2");
        fileDetail.setData(fileDetailData);

        return fileDetail;
    }

    private FileDetailResponse mockExistingQmDetailResponse() {
        FileDetailResponse fileDetail = new FileDetailResponse();
        fileDetail.setStatus(0);
        fileDetail.setMessage("message");

        FileDetailData fileDetailData = new FileDetailData();
        fileDetailData.setEditVersion(0);
        fileDetailData.setData("topicName.status=on\ntopicName.dbName=db1,db2\ntopicName.tableName=t1,t2");
        fileDetail.setData(fileDetailData);

        return fileDetail;
    }

    @Test
    public void testFilterTablesWithAnotherMqInQConfig() {
        String a = "ord_custompaymentdetail,cii_orders,ord_price_deduction,ord_order_vcccardinfo,ord_allneedguarantee_date,riskcontrolresult,ord_addinfo,order_send,ord_backupx_subordermapper,ord_deduct_roominventory,cus_order_amount,ord_inventorystatus,cus_paymentextinfo,ord_soaautosenddata,cus_order_modify_item,ord_hotelstoreorder_info,ord_pendingorder,ord_errorinfo,ord_freeroomcostpriceinfo,ord_groupmembers,ord_coupon_dailyinfo,ord_invoice_elec,ord_roborder_childmaster_relation,cus_payment_partrefund,ord_newticketstatus,cus_submit_hold_order,ord_deposit_policyinfo,hotelorderpromotioninfo,cii_refund,ord_policychild,ord_guest_sms,ord_hotelstore_ticket,ord_tripcost,cus_mileageinfo,ord_auto_confirm,ord_soaautosend,ord_remarkoption_change,ord_didmark,ord_orderfeedetail,ord_unionmember_dailyinfo,ord_bedinfosnapshot,ord_holdorder,ord_ladderdeductpolicy,cus_order_tag,ord_syncordertoebk,ord_createorder_failure_record,ord_suppliersetting,ord_hourroomstayperiod,ord_freeroom,ord_noshowdetail,ord_allianceorderprincebyqe,ord_ltp_refund_distributionord,cii_webfax_list,ord_onlineconfirmorder,cii_orders2,cus_lock_order,ord_paymentinfodetail,ord_associatemember_hotelcommissioninfo,ord_customer_check_in_info,ord_paymentwayinfo,ord_guaranteeinfo,cus_coupon_dailyinfo,cus_hotelroom_info,ord_ltp_paymentrefundtype,ord_preprocessorders,ord_roomgiftlist,ord_lastcanceltimelct,ord_complex_bedtype,ord_merchant_self_service_history,sup_order_extra_fee,ord_mileageinfo,ord_orders_hotelcheckinpolicy,ord_confirmplatform_record,cii_refundmileage,ord_submittedorder,ord_roompricebeforetax,ord_roominventory,ord_children_fee,ord_bidorder,ord_checkavailrateplanprice,cus_operate_time,accjob_fgnoshowrefundcreditcardinfo_history,ord_sendorder,ord_drop_log,ord_orders_ticketgifts_status,fgauditroom,ord_alliancepromotion_dailyinfo,ord_makeuppriceinfo,ord_prepaycancelorder,sup_order_modify_item,cus_order_modify_history,ord_orderstatus_info,ord_processtypestatus,ord_order_masterchildrelationshipinfo,ord_hotel_group_unit_detail,ord_resourcestags,ord_groupmembershipinfo,cus_payment_actioninfo,paymentbillinfo,cii_fax_receive,accjob_fgcheckinorder_history,ord_mileagepointinfolog,ord_intinfo,ord_entervirtualcancelpool_fail_record,ord_pkgorder_switchreceived,ord_repurchase_info,ord_multidaybooksplitorderinfo,ord_didcontact,ord_vatinvoice,ord_promotion_memberrewardinfo,riskcontrolresultsetting,he_create_order_record,ord_freeroom_batchcost,sup_order_ext,ord_compensation_amount,ord_ordercharityinfo,ord_ordertimezone,ord_pointsinfo,ord_roborder_masterorder,orderpointprocess,ord_ordertime,ord_promtos_cgdiscount,ord_ltp_paymentcancledeductinfo,ord_checkavailhotelstays,ord_hqsorders,ord_alliancepromtos_cgdiscount,ord_shadowpriceorder,ord_intelligentremark,ord_invoice_info,ord_orders_newticketgifts,ord_locationprobe_info,ord_orders_usingcoupon_afterreturn_job,ord_hotelselfserviceuserhistory,ord_user_property,ord_orderexno_info,ord_patchticket,ord_extraordermileageinfo,ord_mileage_opt,sup_order_mount_info,ord_orders_consumetickets_detail,ord_authorized_deduction,ord_orderfee_change_log,ord_paymentpartrefund,ord_bonus_detail,ord_hotelguidanceprice_daily,cii_cancelorder,cus_authorized_deduction,cus_back_reward,ord_roomprice,ord_orderpaymentstatusinfo,ord_diyorderprocess,ord_userbrowsebehaviorinfo,ord_approvalticket,ord_sendordercallbackdetail,ord_promotion_totalinfo,ord_fireworm_info,ord_menu_addtionalinfo,ord_todaydeductorder,ord_room_applicativearea,ord_orderhotelofficial,accjob_fgcheckinorder,ord_promotion_memberreward_pointinfo,ord_orderamount,ord_payment_actioninfo,ord_alliancepromotion_totalinfo,paymenttransinfo,ord_room_property,ticketmailmark,ord_orders_pp,ord_roborder_price,ord_alliance_room_price,ord_orderfee_current,ord_roborder_masterchild_relation,ord_policychild_detail,ord_hotelstore_order,ord_orderfee,cus_order_client_detail,ord_checkavailroomprice,ord_intlinformation,ord_promotion_ruletypeinfo,accjobhtlorderstatus,ord_remarkoption,ord_invoice_bookinginfo,ord_mask_campaign_info,ord_order_state,orderpaymentpolicy,ord_textcontent_lang,htlcorpaudit_status,ord_currency_exchange,ord_orderitems,usercredit_amountlimitjobhistory,ord_compensation,ord_additional_commission,ord_ltp_orderpaymentway,ord_virtualcancelpool,cus_order_ext,ord_checkavail_extracharge";
        String[] aa = a.split(",");
        Map<String, String> originalConfig = Maps.newLinkedHashMap();
        List<TableSchemaName> matchTables = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            originalConfig.put("topic" + i + ".status", "on");
            Set<String> dbSet = Sets.newHashSet();
            for (int j = 0; j < 1000; j++) {
                dbSet.add("db" + i + "-" + j);
            }
            originalConfig.put("topic" + i + ".dbName", String.join(",", dbSet));
            Set<String> tblSet = Sets.newHashSet();
            for (int j = 0; j < 1000; j++) {
                tblSet.add("tbl" + i + "-" + j);
                matchTables.add(new TableSchemaName("db" + i + "-" + j, "tbl" + i + "-" + j));
            }
            originalConfig.put("topic" + i + ".tableName", String.join(",", tblSet));
        }

        for (int i = 0; i < 50; i++) {
            originalConfig.put("topictopic-" + i + ".status", "off");
            originalConfig.put("topictopic-" + i + ".dbName", "dbdb-" + i);
            originalConfig.put("topictopic-" + i + ".tableName", "tbltbl-" + i);
            matchTables.add(new TableSchemaName("dbdb-" + i, "tbltbl-" + i));
        }

        for (int i = 0; i < 500; i++) {
            matchTables.add(new TableSchemaName("db-new" + i, "tbl-new" + i));
        }

        originalConfig.put("testtopic.status", "off");
        originalConfig.put("testtopic.dbName", "testdb");
        originalConfig.put("testtopic.tableName", "testtbl");
        matchTables.add(new TableSchemaName("testdb", "testtbl"));

        List<TableSchemaName> res = qConfigService.filterTablesWithAnotherMqInQConfig(originalConfig, matchTables, "testtopic");
        Assert.assertEquals(551, res.size());
    }
}