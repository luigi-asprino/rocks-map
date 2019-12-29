package it.cnr.istc.stlab.rocksmap.test;

import java.util.Random;

import org.junit.Test;
import org.rocksdb.RocksDBException;

import it.cnr.istc.stlab.rocksmap.RocksMultiMap;
import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformer;
import it.cnr.istc.stlab.rocksmap.transformer.StringRocksTransformer;

public class TestRocksMultiMap {

	@Test
	public void speedTest() throws RocksDBException {
		TestUtils.clearTestFolder();
		Long t0 = System.currentTimeMillis();
		RocksMultiMap<Long, String> map = new RocksMultiMap<>(TestUtils.TEST_FOLDER_PATH + "/multimap",
				new LongRocksTransformer(), new StringRocksTransformer());
		Random r = new Random();
		long max = 5 * 1000000;
		for (int i = 0; i < max; i++) {
			if (i % 10000 == 0) {
				System.out.println(i);
			}
			Long key = r.nextLong() % max;
			String value = r.nextLong() % max + "";
			map.containsKey(key);
			map.put(key, value);
			map.get(key);
		}
		map.close();
		Long t1 = System.currentTimeMillis();
		System.out.println((t1 - t0) + "ms");
		TestUtils.clearTestFolder();
	}

}
