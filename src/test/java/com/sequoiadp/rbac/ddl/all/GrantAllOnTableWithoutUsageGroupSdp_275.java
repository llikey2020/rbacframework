package com.sequoiadp.rbac.ddl.all;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestCleanDBBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT ALL ON TABLE TO GROUP syntax validation 
 * @Author        : Lena
 */

public class GrantAllOnTableWithoutUsageGroupSdp_275 extends SDPTestCleanDBBase {
    public GrantAllOnTableWithoutUsageGroupSdp_275() {
        super.setTableName("tablea");
        super.notUsage();
        super.hasGroup();        
    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*does not have usage privilege on.*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("testUser"));
            st1.executeQuery(addgpusersql);
            String grantsql = HiveConnection.getInstance().grantSql("all","table",tableName,"group",getConfig("testGroup"));
            st1.executeQuery(grantsql);
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
           
            st2.executeQuery(usagesql);
            
            String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"),tableName);
            st2.executeQuery(selectsql);
            
            String insertsql = "insert into " + tableName + " values(1003);";
            st2.executeQuery(insertsql);
            
            String updatesql = "update " + tableName + " set  id = 1002 where id = 1003;";
            st2.executeQuery(updatesql);

            String delsql = "delete from " + tableName + " where id = 1001;";
            st2.executeQuery(delsql);

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            if(st2 != null) st2.close();
            conn1.close();
            if(conn2 != null) conn2.close();
        }
    }
}
