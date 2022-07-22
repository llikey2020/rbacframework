package com.sequoiadp.rbac.ddl.privilege_concurrency;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.testng.annotations.Test;
import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : Privilege concurrency verification owner
 * @Author        : Lena
 */

public class PrivilegeConcurrencyVerificationOwner_Sdp_363 extends SDPTestBase {
	
	 public PrivilegeConcurrencyVerificationOwner_Sdp_363() {
	        super.setTableName("tablea");
	    }		
	public static final String DBNAME = "newdbname";
	public static final String TABLENAME = "newtablename";
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*does not have select privilege on table .*")
	public void test() throws SQLException {
		Connection conn1 = null, conn2 = null, conn3 = null;
		Statement st1 = null, st2 = null, st3 = null;
		try {
			// 管理员连接到thriftserver
			conn1 = HiveConnection.getInstance().getAdminConnect();
			st1 = conn1.createStatement();
			String droptable = "drop table if exists " + DBNAME + "." + TABLENAME;    
			String dropdbsql = "drop database if exists " + DBNAME +  ";";
			
			st1.executeQuery(droptable);
			st1.executeQuery(dropdbsql);
			String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));

			st1.executeQuery(usagesql);
			
            String grantsql = HiveConnection.getInstance().grantSql("create","database",DBNAME,"user",getConfig("testUser"));
            st1.executeQuery(grantsql);
           
            String grantsqlall = HiveConnection.getInstance().grantSql("all","database",DBNAME,"user",getConfig("testUser"));
            st1.executeQuery(grantsqlall);
            
            String grantsqlusage = HiveConnection.getInstance().grantSql("usage","database",DBNAME,"user",getConfig("nonowner"));
            st1.executeQuery(grantsqlusage);
            
			// 测试用户连接到thriftserver
			conn2 = HiveConnection.getInstance().getTestConnect();
			st2 = conn2.createStatement();
			
            String createdbsql = "create database " + DBNAME;
            st2.executeQuery(createdbsql);
            
            //variable
            String usagesqldb = HiveConnection.getInstance().usageSql(DBNAME);
            st2.executeQuery(usagesqldb); 
            
            String s3 = "s3a://sdbbucket2/" + TABLENAME;
            //建表

            String createtablesql = "create table " + DBNAME + "." + TABLENAME + "(id int)using delta location \"" + s3 + "\" " + ";" ;
            st2.executeQuery(createtablesql);	
            
            String grantsqlselect = HiveConnection.getInstance().grantSql("select","table",TABLENAME,"user",getConfig("nonowner"));
            st2.executeQuery(grantsqlselect);
            
            conn3 = HiveConnection.getInstance().getNonownerConnect();
            st3= conn3.createStatement();
            
            st3.executeQuery(usagesqldb);
                        
			String selectsql = HiveConnection.getInstance().selectTv(DBNAME, TABLENAME);
			st3.executeQuery(selectsql);
			
			// super user make permission change     
			st1.executeQuery(usagesqldb);
			String revokesql = HiveConnection.getInstance().revokeSql("select","table",TABLENAME,"user",getConfig("nonowner"));
            st1.executeQuery(revokesql);
		
			st3.executeQuery(selectsql);
			

		} catch (SQLException e) {               
			e.printStackTrace();
			throw e;
		} finally {
			st1.close();
			st2.close();
			st3.close();
			conn1.close();
			conn2.close();
			conn3.close();
		}
	}
}
