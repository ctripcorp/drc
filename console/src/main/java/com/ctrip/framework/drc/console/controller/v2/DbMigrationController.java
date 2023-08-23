package com.ctrip.framework.drc.console.controller.v2;




import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.framework.drc.console.service.v2.dbmigration.DbMigrationService;
import com.ctrip.framework.drc.console.vo.display.MigrationTaskVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.PageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
/**
 * @ClassName DbMigrationController
 * @Author haodongPan
 * @Date 2023/8/14 11:30
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v2/dbmigration/")
public class DbMigrationController {

    private static final Logger logger = LoggerFactory.getLogger(DbMigrationController.class);
    @Autowired
    private DbMigrationService dbMigrationService;


    @PostMapping("check")
    public ApiResult dbMigrationCheckAndInit(@RequestBody DbMigrationParam dbMigrationParam) {
        return null;
    }
    
    @GetMapping("query")
    @SuppressWarnings("unchecked")
    public ApiResult<PageResult<MigrationTaskVo>> queryByPage(MigrationTaskQuery queryDto) {
        logger.info("[meta] get allOrderedGroup,drcGroupQueryDto:{}", queryDto);
        if (queryDto == null) {
            queryDto = new MigrationTaskQuery();
        }
        try {
            queryDto.clean();
            PageResult<MigrationTaskTbl> tblPageResult = dbMigrationService.queryByPage(queryDto);
            if (tblPageResult.getTotalCount() == 0) {
                return ApiResult.getSuccessInstance(PageResult.emptyResult());
            }
            List<MigrationTaskTbl> data = tblPageResult.getData();
            List<MigrationTaskVo> res = data.stream().map(MigrationTaskVo::from).collect(Collectors.toList());

            return ApiResult.getSuccessInstance(
                    PageResult.newInstance(res, tblPageResult.getPageIndex(), tblPageResult.getPageSize(), tblPageResult.getTotalCount())
            );
        } catch (Throwable e) {
            logger.error("queryByPage error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("allStatus")
    @SuppressWarnings("unchecked")
    public ApiResult<List<String>> allStatus() {
        List<String> statusList = Arrays.stream(MigrationStatusEnum.values())
                .map(MigrationStatusEnum::getStatus)
                .collect(Collectors.toList());

        return ApiResult.getSuccessInstance(statusList);
    }
}
