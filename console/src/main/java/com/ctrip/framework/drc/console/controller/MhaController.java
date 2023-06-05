package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.dto.MhaDto;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.MhaService;
import com.ctrip.framework.drc.console.service.MySqlService;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;

import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.response.GtidCheckResVo;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;



/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-08-04
 */
@RestController
@RequestMapping("/api/drc/v1/mha/")
public class MhaController {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Autowired private MhaService mhaService;

    @Autowired private MetaInfoServiceImpl metaInfoService;
    
    @Autowired private MySqlService mySqlService;
    
    @Autowired private MonitorTableSourceProvider monitorTableSourceProvider;
    
    
    
    @PostMapping()
    public ApiResult recordMha(@RequestBody MhaDto mhaDto) {
        return mhaService.recordMha(mhaDto);
    }
    
    /**
     * Get all the mha names
     *
     */
    @GetMapping("mhanames")
    public ApiResult getAllClusterNames(@RequestParam(value = "keyWord", defaultValue = "") String keyWord){
        logger.info("Getting all the cluster names...");
        return ApiResult.getSuccessInstance(mhaService.getCachedAllClusterNames(keyWord));
    }

    /**
     * Get all the db names of one specific mha
     *
     */
    @GetMapping("dbnames/cluster/{clusterName}/env/{env}")
    public ApiResult getDbNames(@PathVariable String clusterName, @PathVariable String env){
        logger.info("Getting all the db names...clusterName:" + clusterName + ";env=" + env);
        return ApiResult.getSuccessInstance(mhaService.getAllDbs(clusterName, env));
    }

    /**
     * Get all the db names and dal cluster names of one specific mha
     *
     */
    @GetMapping("dbnames/dalnames/cluster/{clusterName}/env/{env}/zoneId/{zoneId}")
    public ApiResult getAllDbsAndDals(@PathVariable String clusterName, @PathVariable String env, @PathVariable String zoneId){
        logger.info("Getting all the db names and dal names...clusterName:" + clusterName + ";env=" + env + ";zoneId=" + zoneId);
        return ApiResult.getSuccessInstance(mhaService.getAllDbsAndDals(clusterName, env, zoneId));
    }

    @GetMapping("dbnames/dalnames/cluster/{clusterName}/env/{env}")
    public ApiResult getAllDbsAndDals(@PathVariable String clusterName, @PathVariable String env){
        logger.info("Getting all the db names and dal names...clusterName:" + clusterName + ";env=" + env);
        String p = "{\"columns\":[\"UID\"],\"context\":\"SIN\"}";
        return ApiResult.getSuccessInstance(mhaService.getAllDbsAndDals(clusterName, env));
    }

    @GetMapping("uuid/{mhas}/{ip}/{port}/{master}")
    public ApiResult getRealUuid(@PathVariable String mhas,@PathVariable String ip,@PathVariable int port,@PathVariable boolean master){
        try {
            logger.info("Getting getRealUuid from {}:{} in mhas:{}",ip,port,mhas);
            String[] mhaArrs = mhas.split(",");
            if (mhaArrs.length == 2) {
                MhaGroupTbl mhaGroup = metaInfoService.getMhaGroup(mhaArrs[0], mhaArrs[1]);
                return ApiResult.getSuccessInstance(
                        MySqlUtils.getUuid(ip,port,mhaGroup.getReadUser(),mhaGroup.getReadPassword(),master)
                );
            } else {
                return ApiResult.getSuccessInstance(
                        MySqlUtils.getUuid(
                                ip,
                                port,
                                monitorTableSourceProvider.getReadUserVal(),
                                monitorTableSourceProvider.getReadPasswordVal(),
                                master
                        ));
            }
        } catch (Throwable e) {
            logger.error("Getting getRealUuid from {}:{} in mhas:{} error",ip,port,mhas,e);
        }
        return ApiResult.getFailInstance(null);
    }
    
    @GetMapping("gtid/drcExecuted")
    public ApiResult getDrcExecutedGtid(@RequestParam String mha){
        try {
            String unionGtid = mySqlService.getDrcExecutedGtid(mha);
            if (StringUtils.isEmpty(unionGtid)) {
                return ApiResult.getFailInstance(null,"result is empty");
            } else {
                return ApiResult.getSuccessInstance(unionGtid);
            }
        } catch (Throwable e) {
            logger.error("[[tag=gtidQuery]] getDrcExecutedGtid from mha: {}",mha,e);
            return ApiResult.getFailInstance(e,"unexpected exception");
        }
    }

    @GetMapping("gtid/executed")
    public ApiResult getMhaExecutedGtid(@RequestParam String mha){
        try {
            String unionGtid = mySqlService.getMhaExecutedGtid(mha);
            if (StringUtils.isEmpty(unionGtid)) {
                return ApiResult.getFailInstance(null,"result is empty");
            } else {
                return ApiResult.getSuccessInstance(unionGtid);
            }
        } catch (Throwable e) {
            logger.error("[[tag=gtidQuery]] getMhaExecutedGtid from mha: {}",mha,e);
            return ApiResult.getFailInstance(e,"unexpected exception");
        }
    }


    @GetMapping("gtid/purged")
    public ApiResult getMhaPurgedGtid(@RequestParam String mha){
        try {
            String purgedGtid = mySqlService.getMhaPurgedGtid(mha);
            if (StringUtils.isEmpty(purgedGtid)) {
                return ApiResult.getFailInstance(null,"result is empty");
            } else {
                return ApiResult.getSuccessInstance(purgedGtid);
            }
        } catch (Throwable e) {
            logger.error("[[tag=gtidQuery]] getMhaPurgedGtid from mha: {}",mha,e);
            return ApiResult.getFailInstance(e,"unexpected exception");
        }
    }


    @GetMapping("gtid/checkResult")
    public ApiResult getGtidCheckResult(@RequestParam String mha,@RequestParam String configGtid){
        try {
            String purgedGtid = mySqlService.getMhaPurgedGtid(mha);
            GtidSet purgedGtidSet = new GtidSet(purgedGtid);
            boolean legal = purgedGtidSet.isContainedWithin(configGtid);
            GtidCheckResVo resVo = new GtidCheckResVo(legal, purgedGtid);
            return ApiResult.getSuccessInstance(resVo);
        } catch (Throwable e) {
            logger.error("[[tag=gtidQuery]] getGtidCheckResult from mha: {},configGtid:{}",mha,configGtid,e);
            return ApiResult.getFailInstance(e,"unexpected exception");
        }
    }


}
