package com.sequoiadp.rbac.ddl.read_metadata;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : EXPLAIN statement without read_metadata privilege ON table (group)
 * @Author        : Lena
 */

public class ExplainWithoutRead_metadataOnTableGroupSdp_318_B extends SDPTestBase {
    public ExplainWithoutRead_metadataOnTableGroupSdp_318_B() {
        super.setTableName("tablea");
        super.hasGroup();
    }
	
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*does not have read_metadata privilege.*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantsqltable = HiveConnection.getInstance().grantSql("select","table",tableName,"user",getConfig("testUser"));
            st1.executeQuery(grantsqltable);
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("testUser"));
            st1.executeQuery(addgpusersql);

            //测试用户test来验证管理员的语句
            String selectsqltable = HiveConnection.getInstance().selectTv(getConfig("dbName"),tableName);
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();            
            String explainsql = "explain " + selectsqltable;
            st2.executeQuery(explainsql);

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            st2.close();
            conn1.close();
            conn2.close();
        }
    }
}
