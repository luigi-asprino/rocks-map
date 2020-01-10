package it.cnr.istc.stlab.rocksmap.test;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.stream.LongStream;

import org.junit.Test;

import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformer;
import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformerByteBuffer;

public class TestTransformers {

	@Test
	public void testLongTransformerBatch() {
		LongRocksTransformer t = new LongRocksTransformer();
		LongRocksTransformerByteBuffer bb = new LongRocksTransformerByteBuffer();
		for (Long l = -10000L; l < 1000; l++) {
			assertEquals(l, t.transform(t.transform(l)));
			assertEquals(l, bb.transform(bb.transform(l)));
		}
		Random r = new Random(System.currentTimeMillis());
		for (int i = 0; i < 10000; i++) {
			Long l = r.nextLong();
			assertEquals(l, t.transform(t.transform(l)));
			assertEquals(l, bb.transform(bb.transform(l)));
		}
	}

	@Test
	public void testLongTransformParallel() {
		LongRocksTransformer t = new LongRocksTransformer();
		LongRocksTransformerByteBuffer bb = new LongRocksTransformerByteBuffer();
		Random r = new Random(System.currentTimeMillis());
		LongStream.range(0, 100000).parallel().forEach(l -> {
			assertEquals((Long) l, t.transform(t.transform(l)));
			assertEquals((Long) l, bb.transform(bb.transform(l)));
			Long l1 = r.nextLong();
			assertEquals(l1, t.transform(t.transform(l1)));
			assertEquals((Long) l1, bb.transform(bb.transform(l1)));
		});
	}

}
