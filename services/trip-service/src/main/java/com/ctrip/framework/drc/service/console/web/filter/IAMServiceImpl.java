package com.ctrip.framework.drc.service.console.web.filter;


import com.ctrip.basebiz.offline.iam.apifacade.contract.IAMFacadeServiceClient;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.ResultItem;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.VerifyByBatchCodeRequestType;
import com.ctrip.basebiz.offline.iam.apifacade.contract.authorization.VerifyByBatchCodeResponseType;
import com.ctrip.framework.drc.core.service.user.IAMService;
import com.ctrip.framework.drc.service.config.QConfig;
import com.ctrip.infosec.sso.client.CtripSSOTools;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctriposs.baiji.rpc.common.types.AckCodeType;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName IAMFilterService
 * @Author haodongPan
 * @Date 2023/10/23 17:16
 * @Version: $
 */
public class IAMServiceImpl implements IAMService,ConfigChangeListener {

    private IAMFacadeServiceClient iamSoaService = IAMFacadeServiceClient.getInstance();
    private static final String CONFIG_FILE_NAME = "iamfilter.properties";
    private static final String IAM_FILTER_SWITCH = "iam.filter.switch";
    private static final String QUERY_ALL_DB_PERMISSION_CODE = "query.all.db.permission.code";
    private QConfig qConfig;
    private Set<String> apiPrefixSet;
    private  final Logger logger = LoggerFactory.getLogger(IAMFilter.class);

    public IAMServiceImpl() {
        if ("off".equalsIgnoreCase(System.getProperty("iam.config.enable"))) { // for test
            return;
        } 
        qConfig = new QConfig(CONFIG_FILE_NAME);
        qConfig.addConfigChangeListener(this);
        cacheAllKey();
    }
    
    @Override
    public void onChange(String key, String oldValue, String newValue) {
        cacheAllKey();
    }

    @Override
    public Pair<Boolean,String> canQueryAllDbReplication() {
        String permissionCode = qConfig.get(QUERY_ALL_DB_PERMISSION_CODE, "");
        return checkPermission(Lists.newArrayList(permissionCode), CtripSSOTools.getEid());
    }

    @Override
    public Pair<Boolean,String> checkPermission(List<String> permissionCodes, String eid) {
        VerifyByBatchCodeRequestType verifyRequest = new VerifyByBatchCodeRequestType(Lists.newArrayList(permissionCodes), eid);
        try {
            VerifyByBatchCodeResponseType verifyResponse = iamSoaService.verifyByBatchCode(verifyRequest);
            if (!verifyResponse.getResponseStatus().ack.equals(AckCodeType.Success)) {
                logger.error("verifyByBatchCode error,request:{}",verifyRequest);
                return Pair.of(false, "verifyByBatchCode error");
            }
            Map<String, ResultItem> results = verifyResponse.getResults();
            for (Entry<String, ResultItem> res : results.entrySet()) {
                String permissionCode = res.getKey();
                ResultItem resultItem = res.getValue();
                if (!resultItem.hasPermission) {
                    logger.info("IAMFilter nopermission eid:{}, permissionCode:{}", eid, permissionCode);
                    return Pair.of(false, permissionCode);
                }
            }
            return Pair.of(true, null);
        } catch (Exception e) {
            logger.error("verifyByBatchCode error,request:{}",verifyRequest,e);
            return Pair.of(false, "verifyByBatchCode error");
        }
    }

    @Override
    public String matchApiPermissionCode(String requestURL) {
        String api = removeDomain(requestURL);
        String matchedKey = null;
        for (String apiPrefix : apiPrefixSet) {
            if (apiPrefix.endsWith("*")) {
                if (api.startsWith(apiPrefix.substring(0, apiPrefix.length() - 1))) {
                    matchedKey = apiPrefix;
                    break;
                }
            } else {
                if (api.equals(apiPrefix)) {
                    matchedKey = apiPrefix;
                    break;
                }
            }
            
        }
        return matchedKey == null ? null : qConfig.get(matchedKey,null);
    }
    
    @Override
    public boolean iamFilterEnable() {
        String iamFilterSwitch = qConfig.get(IAM_FILTER_SWITCH, "off");
        return iamFilterSwitch.equalsIgnoreCase("on");
    }
    
    private String removeDomain(String requestURL) { 
        return requestURL.substring(requestURL.indexOf("/", 8)); // http:// or https:// -> 8
    }

    private void cacheAllKey() {
        apiPrefixSet = qConfig.getKeys();
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
