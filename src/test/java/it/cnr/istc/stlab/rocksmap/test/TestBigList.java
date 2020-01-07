package it.cnr.istc.stlab.rocksmap.test;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.rocksdb.RocksDBException;

import it.cnr.istc.stlab.rocksmap.RocksBigList;
import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformer;

public class TestBigList {

	@Test
	public void testList() {
		try {
			TestUtils.initTestFolder();

			RocksBigList<Long> list = new RocksBigList<>(TestUtils.TEST_FOLDER_PATH + "/listOfLong",
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

			assertEquals(null, list.get(0));
			assertEquals(0L, list.size64());

//			list.print();

			TestUtils.clearTestFolder();
		} catch (RocksDBException e) {
			e.printStackTrace();
		}

	}

}
