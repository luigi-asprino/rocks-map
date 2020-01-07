package it.cnr.istc.stlab.rocksmap.test;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.rocksdb.RocksDBException;

import it.cnr.istc.stlab.rocksmap.RocksMap;
import it.cnr.istc.stlab.rocksmap.transformer.StringRocksTransformer;

public class TestRocksMap {

	@Test
	public void testMap() {
		RocksMap<String, String> map;
		try {
			TestUtils.initTestFolder();
			map = new RocksMap<>(TestUtils.TEST_FOLDER_PATH + "/test", new StringRocksTransformer(),
					new StringRocksTransformer());
			map.put("k2", "v1");

			assertEquals("v1", map.get("k2"));

			assertEquals(1L, map.sizeLong());
			
			map.put("k3", "v1");
			map.put("k4", "v1");
			
			assertEquals(3L, map.sizeLong());

			TestUtils.clearTestFolder();
		} catch (RocksDBException e) {
			e.printStackTrace();
		}

	}

}
