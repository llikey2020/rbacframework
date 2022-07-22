package com.sequoiadp.testcommon;

import java.sql.SQLException;
import org.testng.annotations.BeforeClass;

public class SDPTestCleanDBBase extends SDPTestBase {
    @BeforeClass
    public void setup() throws SQLException {
		super.recycle();
		super.cleanandbuild();
		super.setup();
	}
}
