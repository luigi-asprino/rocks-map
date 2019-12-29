package it.cnr.istc.stlab.rocksmap.test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.rocksdb.RocksDBException;

import it.cnr.istc.stlab.rocksmap.RocksMap;
import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformer;
import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;
import it.cnr.istc.stlab.rocksmap.transformer.StringRocksTransformer;

public class TestRocksMap {
	public static void testMap() throws RocksDBException {
		RocksTransformer<String> t = new RocksTransformer<String>() {

			@Override
			public byte[] transform(String value) {
				return value.getBytes();
			}

			@Override
			public String transform(byte[] value) {
				return new String(value);
			}

			@Override
			public byte[] transformCollection(Collection<String> value) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Collection<String> transformCollection(byte[] value) {
				// TODO Auto-generated method stub
				return null;
			}
		};

		RocksMap<String, String> map = new RocksMap<>("test", t, t);
		// map.put("k1", "v1");
		map.put("k2", "v1");
		System.out.println(map.containsKey("k1"));
		System.out.println(map.containsKey("k2"));
		System.out.println(map.get("k1"));
		System.out.println(map.get("k2"));
		map.close();
		// System.out.println(map.put("k1", "v2"));
	}

	@SuppressWarnings("unused")
	public static void main(String[] args) throws RocksDBException, IOException {
		StringRocksTransformer srt = new StringRocksTransformer();
		Collection<String> r = new HashSet<String>();
		r.add("a");
		r.add("b");
		r.add("c");
		byte[] c = srt.transformCollection(r);
		Collection<String> r1 = srt.transformCollection(c);
		r1.forEach(r2 -> {
			System.out.println(r2);
		});
		
		
		LongRocksTransformer lrt = new LongRocksTransformer();
		Collection<Long> rl = new HashSet<>();
		rl.add(1L);
		rl.add(2L);
		rl.forEach(r2 -> {
			System.out.println(r2);
		});
	}
}
