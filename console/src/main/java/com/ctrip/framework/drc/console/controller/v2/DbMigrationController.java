package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName DbMigrationController
 * @Author haodongPan
 * @Date 2023/8/14 11:30
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v2/dbmigration/")
public class DbMigrationController {

    @PostMapping("check")
    public ApiResult dbMigrationCheckAndInit(@RequestBody DbMigrationParam dbMigrationParam) {
        return null;
    }
}
