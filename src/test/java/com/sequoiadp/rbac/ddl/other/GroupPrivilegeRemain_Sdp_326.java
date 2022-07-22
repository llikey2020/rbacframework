package com.sequoiadp.rbac.ddl.other;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
/*
 * @Description   : Group privileges remain even the group is assigned to some group that doesn't has
 * @Author        : Lena
 */ 
 
public class GroupPrivilegeRemain_Sdp_326 extends SDPTestBase {
	public GroupPrivilegeRemain_Sdp_326() {
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

			String grantsql = HiveConnection.getInstance().grantSql("select", "table", tableName, "group",
					getConfig("testGroup"));
			st1.executeQuery(grantsql);
			
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("testUser"));
            st1.executeQuery(addgpusersql);

        	// 测试用户连接到thriftserver
			conn2 = HiveConnection.getInstance().getTestConnect();
			st2 = conn2.createStatement();

			String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"), tableName);
			st2.executeQuery(selectsql);
			
			// assign test group to an new group which has no privilege assigned              
            String precreategroup = "drop group "+ "anewgroup" + " ;";
            st1.executeQuery(precreategroup);
            
            String creategroup = "create group "+ "anewgroup" + " ;";
            st1.executeQuery(creategroup);
            
            String addgroupsql = "alter group " + " anewgroup " + " add " + " group " + getConfig("testGroup") + ";"; 
			st1.executeQuery(addgroupsql);
			
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
