package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.console.dto.DataMediaDto;
import com.ctrip.framework.drc.console.service.DataMediaService;
import com.ctrip.framework.drc.core.http.ApiResult;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName DataMediaController
 * @Author haodongPan
 * @Date 2023/1/5 19:37
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v1/dataMedia/")
public class DataMediaController {

    private Logger logger = LoggerFactory.getLogger(getClass());
    
    @Autowired private DataMediaService dataMediaService;


    // -------------------------DataMedia--------------------------------
    @GetMapping("vos")
    public ApiResult getAllDataMediaVos(@RequestParam Long applierGroupId) {
        logger.info("[[query=dataMediaVo]] query applierGroupId:{}", applierGroupId);
        try {
            return ApiResult.getSuccessInstance(dataMediaService.getAllDataMediaVos(applierGroupId));
        } catch (SQLException e) {
            logger.error("[[query=dataMediaVo]] query applierGroupId:{}", applierGroupId,e);
            return ApiResult.getFailInstance("sql error in getAllDataMediaVos");
        }
    }
    
    @GetMapping("properties")
    public ApiResult getProperties(@RequestParam Long applierGroupId) {
        logger.info("[[query=properties]] applierGroupId:{}", applierGroupId);
        try {
            return ApiResult.getSuccessInstance(dataMediaService.generateConfig(applierGroupId));
        } catch (SQLException e) {
            logger.error("[[query=properties]] fail, applierGroupId:{}", applierGroupId,e);
            return ApiResult.getFailInstance("sql error in getProperties");
        }
    }

    @PostMapping("dataMediaConfig")
    public ApiResult inputDataMediaConfig(@RequestBody DataMediaDto dataMediaDto) {
        logger.info("[[meta=dataMediaConfig]] load columnsFilterConfigDto: {}", dataMediaDto);
        try {
            if (dataMediaDto.getId() != 0) {
                return ApiResult.getSuccessInstance(dataMediaService.processUpdateDataMedia(dataMediaDto));
            } else {
                return ApiResult.getSuccessInstance(dataMediaService.processAddDataMedia(dataMediaDto));
            }
        } catch (SQLException e) {
            logger.error("[[meta=dataMediaConfig]] load dataMediaConfig fail with {} ", dataMediaDto, e);
            return ApiResult.getFailInstance("sql error in add or update dataMediaConfig");
        }
    }

    @DeleteMapping("dataMediaConfig/{dataMediaId}")
    public ApiResult deleteDataMediaConfig(@PathVariable Long dataMediaId) {
        logger.info("[[meta=dataMediaConfig]] delete dataMediaConfig id: {}", dataMediaId);
        try {
            return ApiResult.getSuccessInstance(dataMediaService.processDeleteDataMedia(dataMediaId));
        } catch (SQLException e) {
            logger.error("[[meta=dataMediaConfig]] delete dataMediaConfig fail with {} ", dataMediaId, e);
            return ApiResult.getFailInstance("sql error in delete dataMediaConfig");
        }
    }
    
    // todo check dataMedia conflict
    
    
    // -----------------------columnsFilter--------------------------------
    
    @GetMapping("columnsFilterConfig")
    public ApiResult getColumnsFilterConfig(@RequestParam Long dataMediaId) {
        logger.info("[[query=columnsFilterConfig]]  dataMediaId: {}", dataMediaId);
        try {
            if (dataMediaId == 0) {
                return ApiResult.getFailInstance("dataMediaId is 0");
            } else {
                return ApiResult.getSuccessInstance(dataMediaService.getColumnsFilterConfig(dataMediaId));
            }
        } catch (SQLException e) {
            logger.error("[[query=columnsFilterConfig]] fail, dataMediaId: {}", dataMediaId, e);
            return ApiResult.getFailInstance("sql error in getColumnsFilterConfig");
        }
    }
     
    @PostMapping("columnsFilterConfig")
    public ApiResult inputColumnsFilterConfig(@RequestBody ColumnsFilterConfigDto columnsFilterConfigDto) {
        logger.info("[[meta=columnsFilterConfig]] load columnsFilterConfigDto: {}", columnsFilterConfigDto);
        try {
            if (columnsFilterConfigDto.getId() != 0) {
                return ApiResult.getSuccessInstance(dataMediaService.processUpdateColumnsFilterConfig(columnsFilterConfigDto));
            } else {
                return ApiResult.getSuccessInstance(dataMediaService.processAddColumnsFilterConfig(columnsFilterConfigDto));
            }
        } catch (SQLException e) {
            logger.error("[[meta=columnsFilterConfig]] load columnsFilterConfig fail with {} ", columnsFilterConfigDto, e);
            return ApiResult.getFailInstance("sql error in add or update columnsFilterConfig");
        }
    }

    @DeleteMapping("columnsFilterConfig/{columnsFilterId}")
    public ApiResult deleteColumnsFilterConfig(@PathVariable Long columnsFilterId) {
        logger.info("[[meta=columnsFilterConfig]] delete columnsFilterConfig id: {}", columnsFilterId);
        try {
            return ApiResult.getSuccessInstance(dataMediaService.processDeleteColumnsFilterConfig(columnsFilterId));
        } catch (SQLException e) {
            logger.error("[[meta=columnsFilterConfig]] delete columnsFilterConfig fail with {} ", columnsFilterId, e);
            return ApiResult.getFailInstance("sql error in delete columnsFilterConfig");
        }
    }
}
