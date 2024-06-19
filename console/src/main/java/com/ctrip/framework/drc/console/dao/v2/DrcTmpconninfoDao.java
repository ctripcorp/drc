package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.DrcTmpconninfo;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.platform.dal.dao.DalClient;
import com.ctrip.platform.dal.dao.DalClientFactory;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.DalResultSetExtractor;
import com.ctrip.platform.dal.dao.StatementParameters;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.stereotype.Repository;

/**
 * @ClassName DrcTmpconninfoDao
 * @Author haodongPan
 * @Date 2024/6/13 15:35
 * @Version: $
 */
@Repository
public class DrcTmpconninfoDao extends AbstractDao<DrcTmpconninfo> {

    private static final String READ_ACCOUNT_USER= "m_drcv1_r";
    private static final String WRITE_ACCOUNT_USER= "m_drcv1_w";
    private static final String MONITOR_ACCOUNT_USER= "m_drcconsolev1";
    
    public DrcTmpconninfoDao() throws SQLException {
        super(DrcTmpconninfo.class);
    }
    
    //select dbuser,CAST(AES_DECRYPT(Password,'tokenBYDBA') AS Char),DataChange_CreateTime from drc_tmpconninfo where host = ? and port = ?
    public MhaAccounts queryByHostPort(String token,String host, int port) throws SQLException {
        if (host == null || port <= 0) {
            throw new IllegalArgumentException("host or port is null");
        }
        String sql = "select dbuser,CAST(AES_DECRYPT(Password,'%s') AS Char) from drc_tmpconninfo where host = ? and port = ? order by DataChange_CreateTime desc limit 3";
        String formatSql = String.format(sql, token);
        DalClient client = DalClientFactory.getClient("fxdrcmetadb_w");
        StatementParameters parameters = new StatementParameters();
        parameters.set(1, host);
        parameters.set(2, port);
        MhaAccounts mhaAccount = client.query(formatSql, parameters, new DalHints(), new DalResultSetExtractor<MhaAccounts>() {
            @Override
            public MhaAccounts extract(ResultSet rs) throws SQLException {
                MhaAccounts mhaAccounts = new MhaAccounts();
                while (rs.next()) {
                    if (rs.getString(1).equals(READ_ACCOUNT_USER)) {
                        Account account = new Account(rs.getString(1), rs.getString(2));
                        mhaAccounts.setReadAcc(account);
                    } else if (rs.getString(1).equals(WRITE_ACCOUNT_USER)) {
                        Account account = new Account(rs.getString(1), rs.getString(2));
                        mhaAccounts.setWriteAcc(account);
                    } else if (rs.getString(1).equals(MONITOR_ACCOUNT_USER)) {
                        Account account = new Account(rs.getString(1), rs.getString(2));
                        mhaAccounts.setMonitorAcc(account);
                    }

                }
                return mhaAccounts;
            }
        });
        return mhaAccount;
    }
    
    
    
    
}
