package it.cnr.istc.stlab.rocksmap.test;

import java.io.File;
import java.io.IOException;

public class TestUtils {

	public static final String TEST_FOLDER_PATH = "testData";

	public static void clearTestFolder() {
		try {
			org.apache.commons.io.FileUtils.deleteDirectory(new File(TEST_FOLDER_PATH));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
