package it.cnr.istc.stlab.rocksmap.transformer;

import java.util.Collection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public interface RocksTransformer<C> {

	static final ObjectMapper om = new ObjectMapper();

	public C transform(byte[] value);

	public default byte[] transform(C value) {
		byte[] res = null;
		try {
			res = om.writeValueAsBytes(value);
			return res;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return res;
	}

	public default byte[] transformCollection(Collection<C> value) {
		byte[] res = null;
		try {
			res = om.writeValueAsBytes(value);
			return res;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return res;
	}

	public Collection<C> transformCollection(byte[] value);

}
