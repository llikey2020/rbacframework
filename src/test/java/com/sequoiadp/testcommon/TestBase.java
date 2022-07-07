package com.sequoiadp.testcommon;

import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Parameters;

public abstract class TestBase {
    @Parameters({"HOSTNAME", "PORT", "ROOTUSER", "ROOTPWD", "TESTUSER",
            "TESTPWD", "DBNAME", "TESTGROUP","S3BUCKET"})
    @BeforeSuite(alwaysRun = true)
    public static void initSuite(String HOSTNAME, String PORT, String ROOTUSER, String ROOTPWD,
                                 String TESTUSER, String TESTPWD, String DBNAME, String TESTGROUP,String S3BUCKET) {
        ParaBeen.setConfig("hostName",HOSTNAME);
        ParaBeen.setConfig("port",PORT);
        ParaBeen.setConfig("rootUser",ROOTUSER);
        ParaBeen.setConfig("rootPwd",ROOTPWD);
        ParaBeen.setConfig("testUser",TESTUSER);
        ParaBeen.setConfig("testPwd",TESTPWD);
        ParaBeen.setConfig("dbName",DBNAME);
        ParaBeen.setConfig("testGroup",TESTGROUP);
        ParaBeen.setConfig("url","jdbc:hive2://" + HOSTNAME + ":" + PORT );
        ParaBeen.setConfig("S3Bucket",S3BUCKET);
    }
}
