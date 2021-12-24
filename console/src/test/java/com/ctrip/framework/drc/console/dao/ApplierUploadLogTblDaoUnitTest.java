package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ApplierUploadLogTbl;
import com.ctrip.platform.dal.dao.DalClient;
import com.ctrip.platform.dal.dao.DalClientFactory;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import org.junit.*;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

/**
 * JUnit test of ApplierUploadLogTblDao class.
 * Before run the unit test, you should initiate the test data and change all the asserts correspond to you case.
**/
public class ApplierUploadLogTblDaoUnitTest {

	private static final String DATA_BASE = "fxdrcmetadb_w";

	private static DalClient client = null;
	private static ApplierUploadLogTblDao dao = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		/**
		* Initialize DalClientFactory.
		* The Dal.config can be specified from class-path or local file path.
		* One of follow three need to be enabled.
		**/
		DalClientFactory.initClientFactory(); // load from class-path Dal.config
		DalClientFactory.warmUpConnections();
		client = DalClientFactory.getClient(DATA_BASE);
		dao = new ApplierUploadLogTblDao();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {

	}

	@Before
	public void setUp() throws Exception {
//		To prepare test data, you can simply uncomment the following.
//		In case of DB and table shard, please revise the code to reflect shard
		for(long i = 1; i <= 10; i++) {
			ApplierUploadLogTbl daoPojo = createPojo(i);

			try {
				dao.insert(new DalHints().enableIdentityInsert(), daoPojo);
			} catch (SQLException e) {
			}
		}
	}

	private ApplierUploadLogTbl createPojo(long index) {
		ApplierUploadLogTbl daoPojo = new ApplierUploadLogTbl();

		daoPojo.setLogId(index);
		//daoPojo set not null field

		return daoPojo;
	}

	private void changePojo(ApplierUploadLogTbl daoPojo) {
		// Change a field to make pojo different with original one
	}

	private void changePojos(List<ApplierUploadLogTbl> daoPojos) {
		for(ApplierUploadLogTbl daoPojo: daoPojos)
			changePojo(daoPojo);
	}

	private void verifyPojo(ApplierUploadLogTbl daoPojo) {
		//assert changed value
	}

	private void verifyPojos(List<ApplierUploadLogTbl> daoPojos) {
		for(ApplierUploadLogTbl daoPojo: daoPojos)
			verifyPojo(daoPojo);
	}

	@After
	public void tearDown() throws Exception {
//		To clean up all test data
//		dao.delete(null, dao.queryAll(null));
	}

	@Test
	public void testQueryLogByPage() {
	}
}
