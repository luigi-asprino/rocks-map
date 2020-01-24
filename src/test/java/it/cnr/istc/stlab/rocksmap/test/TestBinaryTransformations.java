package it.cnr.istc.stlab.rocksmap.test;

import org.junit.Test;

import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformerByteBuffer;
import it.cnr.istc.stlab.rocksmap.transformer.ObjectRocksOnlineTransformer;

public class TestBinaryTransformations {

	@Test
	public void testBinObject() {
		ObjectRocksOnlineTransformer<Long> t = new ObjectRocksOnlineTransformer<Long>();
		LongRocksTransformerByteBuffer tl = new LongRocksTransformerByteBuffer();
		Long n = 1L;
		long b1, b2;

		b1 = Runtime.getRuntime().freeMemory();
		byte[] l = t.transform(n);
		b2 = Runtime.getRuntime().freeMemory();
		System.out.println(b1 - b2);

		System.gc();

		tl.transform(1L);

		b1 = Runtime.getRuntime().freeMemory();
		byte[] l1 = tl.transform(n);
		b2 = Runtime.getRuntime().freeMemory();
		System.out.println(b1 - b2);

		int c = 0, c1 = 0;
		for (long i = 0; i < 100000; i++) {
			b1 = Runtime.getRuntime().freeMemory();
			byte[] b = tl.transform(n);
			b2 = Runtime.getRuntime().freeMemory();
			if (b1 - b2 != 0) {
				c++;
			}
		}

		for (long i = 0; i < 100000; i++) {
			b1 = Runtime.getRuntime().freeMemory();
			byte[] b3 = t.transform(n);
			b2 = Runtime.getRuntime().freeMemory();
			if (b1 - b2 != 0) {
				c1++;
			}
		}
		System.out.println(c);
		System.out.println(c1);

	}

}
