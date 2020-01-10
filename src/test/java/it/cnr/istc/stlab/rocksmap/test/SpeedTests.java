package it.cnr.istc.stlab.rocksmap.test;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.stream.LongStream;

import org.junit.Test;

import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformer;
import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformerByteBuffer;

public class SpeedTests {

	private static final long NUMBER_OF_TESTS = 100000000;

	@Test
	public void testSpeedLongTransformer() {
		LongRocksTransformer t = new LongRocksTransformer();
		Random r = new Random(System.currentTimeMillis());
		LongStream.range(0, NUMBER_OF_TESTS).parallel().forEach(l -> {
			assertEquals((Long) l, t.transform(t.transform(l)));
			Long l1 = r.nextLong();
			assertEquals(l1, t.transform(t.transform(l1)));
		});
	}

	@Test
	public void testSpeedLongTransformerByteBuffer() {
		LongRocksTransformerByteBuffer bb = new LongRocksTransformerByteBuffer();
		Random r = new Random(System.currentTimeMillis());
		LongStream.range(0, NUMBER_OF_TESTS).parallel().forEach(l -> {
			assertEquals((Long) l, bb.transform(bb.transform(l)));
			Long l1 = r.nextLong();
			assertEquals((Long) l1, bb.transform(bb.transform(l1)));
		});
	}

}
