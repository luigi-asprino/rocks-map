package it.cnr.istc.stlab.rocksmap.transformer;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

public class IntegerRocksTransformer implements RocksTransformer<Integer> {

	private ObjectMapper om = new ObjectMapper();

	@Override
	public Integer transform(byte[] value) {
		return Integer.parseInt(new String(value));
	}

	@Override
	public byte[] transform(Integer value) {
		return value.toString().getBytes();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<Integer> transformCollection(byte[] value) {
		Collection<Integer> r = null;
		try {
			return om.readValue(value, Set.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return r;
	}

}
