package it.cnr.istc.stlab.rocksmap.transformer;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

public class StringRocksTransformer implements RocksTransformer<String> {

	private ObjectMapper om = new ObjectMapper();

	@Override
	public String transform(byte[] value) {
		return new String(value);
	}

	@Override
	public byte[] transform(String value) {
		return value.getBytes();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<String> transformCollection(byte[] value) {
		Collection<String> r = null;
		try {
			r = (Set<String>) om.readValue(value, Set.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return r;
	}

}
