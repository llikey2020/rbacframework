package com.sequoiadp.rbac.ddl.other;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : drop database cascade will drop the privilege which belongs to the user
 * @Author        : Lena
 */

public class DropDBCascadeDropPrivilege_Sdp_364 extends SDPTestBase {
    
	public static final String TABLENAME = "newtablename";
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*Table or view not found.*")
	public void test() throws SQLException {
		Connection conn1 = null, conn2 = null;
		Statement st1 = null, st2 = null;
		try {
			// 管理员连接到thriftserver
			conn1 = HiveConnection.getInstance().getAdminConnect();
			st1 = conn1.createStatement();

			String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
			st1.executeQuery(usagesql);

            String grantsql = HiveConnection.getInstance().grantSql("create","table",TABLENAME,"user",getConfig("testUser"));
            st1.executeQuery(grantsql);
            
            String grantsqlall = HiveConnection.getInstance().grantSql("all","table",TABLENAME,"user",getConfig("testUser"));
            st1.executeQuery(grantsqlall);
            
			// 测试用户连接到thriftserver
			conn2 = HiveConnection.getInstance().getTestConnect();
			st2 = conn2.createStatement();
            
            String s3 = "s3a://sdbbucket2/" + TABLENAME;
            //建表
            String createtablesql = "create table " + getConfig("dbName") + "." + TABLENAME + " (id int)using delta location \"" + s3 + "\" " + ";" ;
            st2.executeQuery(createtablesql);	
            
			String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"), TABLENAME);
			st2.executeQuery(selectsql);
			
			// drop database cascade            
            String dropdb="drop database "+ getConfig("dbName") + " cascade;";
            st1.executeQuery(dropdb);
		
			st2.executeQuery(selectsql);

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			String s3 = "s3a://sdbbucket2/" + tableName;
	        String createdbsql = "create database if not exists " + getConfig("dbName") + ";";
	        st1.executeQuery(createdbsql);
	        String createtablsql = "create table if not exists " + tableName + "(id int)using delta location \"" + s3 + "\" " + ";" ;
	        st1.executeQuery(createtablsql);
			st1.close();
			st2.close();
			conn1.close();
			conn2.close();
		}
	}
}
