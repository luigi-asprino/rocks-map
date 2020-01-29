package it.cnr.istc.stlab.rocksmap.test;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;
import org.rocksdb.RocksDBException;

import it.cnr.istc.stlab.rocksmap.RocksBigList;
import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformer;

public class TestBigList {

	@Test
	public void testList() {
		try {
			TestUtils.initTestFolder();

			RocksBigList<Long> list = new RocksBigList<>(TestUtils.TEST_FOLDER_PATH + "/listOfLong1",
					new LongRocksTransformer());

			assertEquals(0L, list.size64());

			list.add(1L);
			list.add(2L);
			list.add(3L);

			list.print();

			assertEquals(new Long(3), list.get(2));
			assertEquals(3L, list.size64());

			Long r = list.remove(0);

			list.print();

			assertEquals(new Long(1), r);

			list.print();

			assertEquals(new Long(2), list.get(0));
			assertEquals(2L, list.size64());

			list.remove(0);

			assertEquals(new Long(3), list.get(0));
			assertEquals(1L, list.size64());

			list.remove(0);

			assertEquals(0L, list.size64());

			TestUtils.clearTestFolder();
		} catch (RocksDBException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void speedTestList() {
		try {
			TestUtils.initTestFolder();

			RocksBigList<Long> list = new RocksBigList<>(TestUtils.TEST_FOLDER_PATH + "/listOfLong2",
					new LongRocksTransformer());

			long numberOfElements = 100 * 1000000;
			Random r = new Random();
			for (long i = 1; i < numberOfElements; i++) {
				list.add(r.nextLong());
				list.get(Math.abs(r.nextLong()) % i);
			}

			TestUtils.clearTestFolder();
		} catch (RocksDBException e) {
			e.printStackTrace();
		}

	}

}
