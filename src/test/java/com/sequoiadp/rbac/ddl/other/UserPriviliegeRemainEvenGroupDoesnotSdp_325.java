package com.sequoiadp.rbac.ddl.other;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
/*
 * @Description   : Users privilege remain even the group doesn't has   
 * @Author        : Lena
 */
public class UserPriviliegeRemainEvenGroupDoesnotSdp_325 extends SDPTestBase {
	public UserPriviliegeRemainEvenGroupDoesnotSdp_325() {
		super.setTableName("tablea");
		super.hasGroup();
	}

	 @Test
	public void test() throws SQLException {
		Connection conn1 = null, conn2 = null;
		Statement st1 = null, st2 = null;
		try {
			// 管理员连接到thriftserver
			conn1 = HiveConnection.getInstance().getAdminConnect();
			st1 = conn1.createStatement();

			String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
			st1.executeQuery(usagesql);

			String grantsql = HiveConnection.getInstance().grantSql("select", "table", tableName, "user",
					getConfig("testUser"));
			st1.executeQuery(grantsql);
			
			// 测试用户连接到thriftserver
			conn2 = HiveConnection.getInstance().getTestConnect();
			st2 = conn2.createStatement();

			String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"), tableName);
			st2.executeQuery(selectsql);
			
			//Adding user to a group which doesn't has any privilege
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("testUser"));
            st1.executeQuery(addgpusersql);
            
			//User privilege remain
			st2.executeQuery(selectsql);			
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			st1.close();
			st2.close();
			conn1.close();
			conn2.close();
		}
	}
}
