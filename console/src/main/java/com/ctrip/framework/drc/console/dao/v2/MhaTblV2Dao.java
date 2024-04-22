package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.param.v2.MhaQuery;
import com.ctrip.framework.drc.console.param.v2.MhaQueryParam;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.MatchPattern;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/25 12:04
 */
@Repository
public class MhaTblV2Dao extends AbstractDao<MhaTblV2> {

    private static final String MHA_NAME = "mha_name";
    private static final String BU_ID = "bu_id";
    private static final String DC_ID = "dc_id";
    private static final String ID = "id";
    private static final String DELETED = "deleted";

    public MhaTblV2Dao() throws SQLException {
        super(MhaTblV2.class);
    }

    public MhaTblV2 queryByMhaName(String mhaName) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(MHA_NAME, mhaName, Types.VARCHAR);
        return client.queryFirst(sqlBuilder, new DalHints());
    }

    public List<MhaTblV2> queryByMhaNames(List<String> mhaNames) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(MHA_NAME, mhaNames, Types.VARCHAR);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<MhaTblV2> queryByMhaNames(List<String> mhaNames, int deleted) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll()
                .equal(DELETED, deleted, Types.TINYINT).and()
                .in(MHA_NAME, mhaNames, Types.VARCHAR);
        return client.query(sqlBuilder, new DalHints());
    }

    public MhaTblV2 queryByMhaName(String mhaName, int deleted) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(DELETED, deleted, Types.TINYINT).and().equal(MHA_NAME, mhaName, Types.VARCHAR);
        return client.queryFirst(sqlBuilder, new DalHints());
    }

    public List<MhaTblV2> queryByDcId(Long dcId, int deleted) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll()
                .equal(DELETED, deleted, Types.TINYINT).and()
                .equal(DC_ID, dcId, Types.BIGINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<MhaTblV2> query(MhaQuery mhaQuery) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and()
                .likeNullable(MHA_NAME, mhaQuery.getContainMhaName(), MatchPattern.CONTAINS, Types.VARCHAR).and()
                .equalNullable(BU_ID, mhaQuery.getBuId(), Types.BIGINT).and()
                .inNullable(DC_ID, mhaQuery.getDcIdList(), Types.BIGINT);
        return client.query(sqlBuilder, new DalHints());
    }

    @Override
    public List<MhaTblV2> queryByIds(List<Long> ids) throws SQLException {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        }
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().in(ID, ids, Types.BIGINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<MhaTblV2> queryByParam(MhaQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.selectCount();
        int count = client.count(sqlBuilder, new DalHints()).intValue();
        param.getPageReq().setTotalCount(count);

        sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.selectAll().atPage(param.getPageReq().getPageIndex(), param.getPageReq().getPageSize()).orderBy(ID, false);
        return queryList(sqlBuilder);
    }

    private SelectSqlBuilder buildSqlBuilder(MhaQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        if (StringUtils.isNotBlank(param.getMhaName())) {
            sqlBuilder.and().like(MHA_NAME, param.getMhaName(), MatchPattern.CONTAINS, Types.VARCHAR);
        }
        if (!CollectionUtils.isEmpty(param.getDcIds())) {
            sqlBuilder.and().in(DC_ID, param.getDcIds(), Types.BIGINT);
        }
        if (!CollectionUtils.isEmpty(param.getMhaIds())) {
            sqlBuilder.and().in(ID, param.getMhaIds(), Types.BIGINT);
        }
        return sqlBuilder;
    }
}
