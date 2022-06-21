package com.sequoiadp.testcommon;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HiveConnection {
    private static HiveConnection instsance;
    private static Lock lock= new ReentrantLock();

    public static HiveConnection getInstance(){
        if(instsance==null){
            instsance=new HiveConnection();
        }
        return instsance;
    }

    //通过驱动连接数据库
    public Connection getAdminConnect () {
        return getConnect(ParaBeen.getConfig("rootUser"),ParaBeen.getConfig("rootPwd"));
    }

    public Connection getTestConnect (){
        return getConnect(ParaBeen.getConfig("testUser"),ParaBeen.getConfig("testPwd"));
    }

    public  Connection getConnect (String user,String pwd) {
        synchronized(lock) {
            Connection conn = null;
            try {
                Class.forName("org.apache.hive.jdbc.HiveDriver");
                conn = DriverManager.getConnection(ParaBeen.getConfig("url"), user, pwd);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            return conn;
        }
    }

    public String grantSql (String privilege,String dbType,String Dtname,String userType,String username){
        StringBuilder sb=new StringBuilder();
        sb.append("grant ").append(privilege).append(" on ").append(dbType).append(' ');
        sb.append(Dtname).append(" to ").append(userType).append(' ').append(username).append(';');
        return sb.toString();
    }

    public  String revokeDbFromUser(String privilege){
        String s = "revoke "+ privilege + " on database " + ParaBeen.getConfig("dbName") + " from user " + ParaBeen.getConfig("testUser") + ";";
        return s;
    }
    public String selectTv (String dbName,String tabname){
        String s = "select * from " + dbName + "." + tabname + ";";
        return s;
    }
    public String usageSql(String dbName){
        return "use " + dbName + ";" ;
    }

    public String alterSql(String st1,String st2,String st3,String st4,String st5){
        return "alter " +st1 + " "+ st2 + " " + st3 + " " +st4  + " " + st5 + ";";
    }

    public String dropSql(String dt,String name){
        return "drop "+ dt + " " + name + ";";
    }
}
