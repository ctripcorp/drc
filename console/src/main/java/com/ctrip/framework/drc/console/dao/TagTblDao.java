package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.TagTbl;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2025/6/17 17:45
 */
@Repository
@Lazy
public class TagTblDao extends AbstractDao<TagTbl> {

    public TagTblDao() throws SQLException {
        super(TagTbl.class);
    }
}
