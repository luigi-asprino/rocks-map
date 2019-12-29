package it.cnr.istc.stlab.rocksmap.transformer;

import java.util.Collection;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Sets;

public class URIStringRocksTransformer implements RocksTransformer<String> {

	public byte[] transform(String value) {
		return value.getBytes();
	}

	public byte[] transformCollection(Collection<String> value) {
		return StringUtils.join(value, ' ').getBytes();
	}

	@Override
	public String transform(byte[] value) {
		return new String(value);
	}

	@Override
	public Collection<String> transformCollection(byte[] value) {
		return Sets.newHashSet(StringUtils.split(new String(value), ' '));
	}

}
