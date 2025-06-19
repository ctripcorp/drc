package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.TagTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2025/6/17 17:45
 */
@Repository
public class TagTblDao extends AbstractDao<TagTbl> {

    public TagTblDao() throws SQLException {
        super(TagTbl.class);
    }
}
