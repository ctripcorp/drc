package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/6/8 14:14
 */
public class NameFilterTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String MONITOR_DB = "drcmonitordb\\.delaymonitor";

    @Test
    public void testSplitNameFilter() {
        String nameFilter = "bbzmembersaccountshard\\\\d*db\\\\.sjp_(uid_accounts|user_password|email_accounts|mobilephone_accounts|thirdparty_accounts)_\\\\d*,bbzaccountsshard\\\\d*db\\\\.sjp_user_accounts_\\\\d*,bbzcredentialshard\\\\d*db\\\\.sjp_credential_keyid_\\\\d*,ttdvbkorderdb\\\\.(vbk_columnconfig|vbk_user_columnconfig|vbk_verificationpoint|vbk_client|vbk_client_modify|vbk_contentextend|vbk_delaytask|vbk_order|vbk_order_apply|vbk_order_cancel|vbk_order_connectfail|vbk_order_coupon|vbk_order_ext|vbk_order_file|vbk_order_installment|vbk_order_payment|vbk_order_recall|vbk_order_resource|vbk_order_ticket|vbk_order_tobedone|vbk_order_verificationcode|vbk_order_verificationlog|vbk_ordercode_tobedone|vbk_orderdeliverinfo|vbk_orderextendproperty|vbk_ordermessage|vbk_orderpricelist|vbk_orderprocesslog|vbk_orderproperties|vbk_orderrefund|vbk_orderrefunditem),fncgcinventorydb.(inventory|inventory_subappxref|inventory_warning|inventoryext|inventorylog|inventoryrecord),ttdbookingsshard0[0-7]db\\\\.(ord_ordertask|ttd_batch_orderinfo|ttd_ordercanbookinginfo|ttd_ordercontentext|ttd_orderinfo),tourttdordershard0[0-7]db\\\\.(ord_allianceorder|ord_contactinfo|ord_customerinfo|ord_insuranceinsured|ord_insuredproposer|ord_msgcontent|ord_msgcontentitem|ord_order|ord_orderactivity|ord_orderaddinfo|ord_orderems|ord_orderitem|ord_orderitemaddinfo|ord_orderitemconfirminfo|ord_orderitemviewspot|ord_orderitem_ext|ord_orderitem_localeinfo|ord_orderitem_snapshot|ord_orderpostpaid|ord_order_ext|ord_promotioninfo|ord_purchaseorder),ttdorderdb\\\\.(ord_commonconfiguration|ord_logisticscompany|sim_voucher|aud_orderrefund|aud_rebatemoney|log_settlementadjustinfo|log_vendoractiondetail|ord_callbackrecord|ord_callrecordrelation|ord_callrecords|ord_checkordertask|ord_commentpointrecord|ord_confirmfailorder|ord_deliverygoods|ord_hastencheckorder|ord_manualorder|ord_mileagerequest|ord_orderattachment|ord_orderattachmentitem|ord_orderchange|ord_orderchangeitems|ord_orderchangeitemslog|ord_orderchangelog|ord_orderchangeresult|ord_orderconfirmfailrecord|ord_orderitemcheckinfo|ord_orderitemdeliveryorm|ord_orderitemsubsidies|ord_orderitemtask|ord_orderrebate|ord_orderrefundcheckinfo|ord_ordertask|ord_precheckfailurerecord|ord_rebatechangelog|ord_rebaterecord|ord_refundautoorder|ord_refunditem|ord_reservedetail|ord_riskordermessage|ord_sendemailattachmentrecords|ord_sendemailrecords|ord_subrefunditem|ord_userbehavior|ord_useroperateorderdetail|ord_useroperateorderinfo|ord_vendorconnectioninfo|ord_vendororderrequest|ord_voucherprint|refund_orderrefundopdetail|refund_orderrefundopinfo),ttdorderprocessdb\\\\.(ord_settlementvendor_black|log_ordertimes|ord_settlementrequest),ttdorderscheduledb\\\\.(ord_autoorder|ord_callouttask|ord_commenttask|ord_invoicetask|ord_mileagetask|ord_ordercheckriskrecord|ord_ordertask|ord_ordsyncmidcompenttask|ord_overduerefundorder|ord_overdue_forcerefund|ord_precheckordertask|ord_refunddeferorder|ord_vouchertask),ttdproductsnapshotdb\\\\.(ord_productsnapshot|ord_resourcesnapshot|ord_cityinfo|ord_customerinfotemplate|ord_customerinfotemplateitem|ord_itineraryinfo|ord_localeproductinfo|ord_ordersnapshot|ord_originalorderinfo|ord_payinfosnapshot|ord_vendorconfirmtimeinfo),tourttdpaymentdb\\\\.(ord_paymentitem|ord_subpaymentitem|pay_billcallbackrequest|pay_payfail|pay_paymentinfo|pay_paymenttempresult|pay_quickpaymentresult|pay_tourtobpaymentresult),ttdorderOfflinedb\\\\.(com_messageprocess|log_businesslog|log_callingleveldetail|log_orderprocess|log_sendsms|ord_checkcustomerinfo),ttdorderfinancedb\\\\.(fin_orderrevenue|fin_orderrevenuelog)";
        List<String> splitDbs = Lists.newArrayList(nameFilter.split(","));
        if (EnvUtils.pro() && (splitDbs.size() <= 1 || !splitDbs.get(0).equals(MONITOR_DB))) {
            logger.error("split nameFilter error");
        }
        splitDbs.remove(MONITOR_DB);
        List<String> dbs = splitDbs.stream().map(db -> splitNameFilter(db)).collect(Collectors.toList());
        String newNameFilter = StringUtils.join(dbs, ",");
        dbs.forEach(db -> {
            String[] dbString = db.split(",");
            for (String str : dbString) {
                System.out.println(str);
            }

        });
        System.out.println(newNameFilter);
    }


    private String splitNameFilter(String db) {
        if (!db.contains("(")) {
            return db;
        }
        String[] dbStrings = db.split("\\.");
        String dbName = dbStrings[0];
        String[] preTableStr = dbStrings[1].split("\\(");
        String prefixTable = preTableStr[0];

        String[] sufTableStr = preTableStr[1].split("\\)");
        String tableFilter = sufTableStr[0];

        String suffixTable = "";
        if (sufTableStr.length == 2) {
            suffixTable = sufTableStr[1];
        }
        String finalSuffixTable = suffixTable;

        String[] tables = tableFilter.split("\\|");
        List<String> dbFilters = Arrays.stream(tables).map(table -> buildNameFilter(dbName, table, prefixTable, finalSuffixTable)).collect(Collectors.toList());
        String newNameFilter = StringUtils.join(dbFilters, ",");
        return newNameFilter;
    }


    private String buildNameFilter(String dbName, String table, String prefixTable, String suffixTable) {
        return dbName + "." + prefixTable + table + suffixTable;
    }

}
