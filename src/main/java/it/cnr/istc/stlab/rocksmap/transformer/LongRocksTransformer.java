package it.cnr.istc.stlab.rocksmap.transformer;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LongRocksTransformer implements RocksTransformer<Long> {

	private ObjectMapper om;

	public LongRocksTransformer() {
		om = new ObjectMapper();
		om.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
	}

	@Override
	public Long transform(byte[] value) {
		return Long.parseLong(new String(value));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<Long> transformCollection(byte[] value) {
		Collection<Long> r = null;
		try {
			r = (Collection<Long>) om.readValue(value, Set.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return r;
	}

	

	

}
