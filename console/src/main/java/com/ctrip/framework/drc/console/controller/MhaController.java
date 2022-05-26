package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.MhaService;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;

import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.driver.healthcheck.task.ExecutedGtidQueryTask;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;


/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-08-04
 */
@RestController
@RequestMapping("/api/drc/v1/mha/")
public class MhaController {

    private final Logger logger = LoggerFactory.getLogger(getClass());


    @Autowired
    private MhaService mhaService;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private DataMediaTblDao dataMediaTblDao;

    private DalUtils dalUtils = DalUtils.getInstance();
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

    @GetMapping("{mhas}/uuid/{ip}/{port}/{master}")
    public ApiResult getRealUuid(@PathVariable String mhas,@PathVariable String ip,@PathVariable int port,@PathVariable boolean master){
        try {
            logger.info("Getting getRealUuid from {}:{} in mhas:{}",ip,port,mhas);
            String[] mhaArrs = mhas.split(",");
            if (mhaArrs.length == 2) {
                MhaGroupTbl mhaGroup = metaInfoService.getMhaGroup(mhaArrs[0], mhaArrs[1]);
                return ApiResult.getSuccessInstance(MySqlUtils.getUuid(ip,port,mhaGroup.getReadUser(),mhaGroup.getReadPassword(),master));
            }
        } catch (Throwable e) {
            logger.error("Getting getRealUuid from {}:{} in mhas:{} error",ip,port,mhas,e);
        }
        return ApiResult.getFailInstance(null);
    }

    @GetMapping("{mhas}/gtid/{mha}")
    public ApiResult getReaExecutedGtid(@PathVariable String mhas,@PathVariable String mha){
        try {
            logger.info("Getting getReaExecutedGtid from {} master in mhas:{}",mha,mhas);
            String[] mhaArrs = mhas.split(",");
            if (mhaArrs.length == 2) {
                MhaGroupTbl mhaGroup = metaInfoService.getMhaGroup(mhaArrs[0], mhaArrs[1]);
                MachineTbl machineTbl = metaInfoService.getMachineTbls(mha).stream().filter(p -> BooleanEnum.TRUE.getCode().equals(p.getMaster())).findFirst().orElse(null);
                if (machineTbl != null) {
                    MySqlEndpoint mySqlEndpoint = new MySqlEndpoint(machineTbl.getIp(), machineTbl.getPort(), mhaGroup.getReadUser(), mhaGroup.getReadPassword(), true);
                    return ApiResult.getSuccessInstance(new ExecutedGtidQueryTask(mySqlEndpoint).call());
                }
                logger.error("Getting getReaExecutedGtid from {} master in mhas:{},machine not exist",mha,mhas);
            }
        } catch (Throwable e) {
            logger.error("Getting getReaExecutedGtid from {} master in mhas:{}",mha,mhas,e);
        }

        return ApiResult.getFailInstance(null);
    }


    @GetMapping("dataMedia/check/{namespace}/{name}/{srcDc}/{dataMediaSourceName}/{type}")
    public ApiResult getMatchTable(@PathVariable String namespace,
                                   @PathVariable String name,
                                   @PathVariable String srcDc,
                                   @PathVariable String dataMediaSourceName,
                                   @PathVariable Integer type) {

        try {
            logger.info("[[tag=matchTable]] get {}.{} from {} ", namespace, name, dataMediaSourceName);
            MhaGroupTbl mhaGroup = metaInfoService.getMhaGroupForMha(dataMediaSourceName);
            MachineTbl machineTbl = metaInfoService.getMachineTbls(dataMediaSourceName).stream()
                    .filter(p -> BooleanEnum.TRUE.getCode().equals(p.getMaster())).findFirst().orElse(null);
            if (machineTbl != null) {
                MySqlEndpoint mySqlEndpoint = new MySqlEndpoint(
                        machineTbl.getIp(),
                        machineTbl.getPort(),
                        mhaGroup.getMonitorUser(),
                        mhaGroup.getMonitorPassword(),
                        true);
                AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(namespace + "\\." + name);
                List<MySqlUtils.TableSchemaName> tables = MySqlUtils.getTablesAfterRegexFilter(mySqlEndpoint, aviatorRegexFilter);
                return ApiResult.getSuccessInstance(tables);
            }
            return ApiResult.getFailInstance("no machine find for " + dataMediaSourceName);
        } catch (Exception e) {
            logger.warn("[[tag=matchTable]] error when get {}.{} from {}", namespace, name, dataMediaSourceName, e);
            if (e instanceof SQLException) {
                return ApiResult.getFailInstance("sql error");
            } else if (e instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else {
                return ApiResult.getFailInstance("other error");
            }
        }

    }

    @GetMapping("rowsFilter/commonColumns/{srcDc}/{srcMha}/{namespace}/{name}")
    public ApiResult getCommonColumnInDataMedias(
            @PathVariable String srcDc,
            @PathVariable String srcMha,
            @PathVariable String namespace,
            @PathVariable String name) {
        try {
            logger.info("[[tag=commonColumns]] get columns {}\\.{} from {}", namespace, name, srcMha);
            MhaGroupTbl mhaGroup = metaInfoService.getMhaGroupForMha(srcMha);
            MachineTbl machineTbl = metaInfoService.getMachineTbls(srcMha).stream()
                    .filter(p -> BooleanEnum.TRUE.getCode().equals(p.getMaster())).findFirst().orElse(null);
            if (machineTbl != null) {
                MySqlEndpoint mySqlEndpoint = new MySqlEndpoint(
                        machineTbl.getIp(),
                        machineTbl.getPort(),
                        mhaGroup.getMonitorUser(),
                        mhaGroup.getMonitorPassword(),
                        true);
                AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(namespace + "\\." + name);
                Set<String> allCommonColumns = MySqlUtils.getAllCommonColumns(mySqlEndpoint, aviatorRegexFilter);
                return ApiResult.getSuccessInstance(allCommonColumns);
            }
            return ApiResult.getFailInstance("no machine find for " + srcMha);
        } catch (Exception e) {
            logger.warn("[[tag=commonColumns]] get columns {}\\.{} from {} error", namespace, name, srcMha, e);
            if (e instanceof SQLException) {
                return ApiResult.getFailInstance("sql error");
            } else if (e instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else {
                return ApiResult.getFailInstance("other error");
            }
        }
    }



}
