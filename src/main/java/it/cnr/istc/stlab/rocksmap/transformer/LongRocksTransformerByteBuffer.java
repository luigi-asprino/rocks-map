package it.cnr.istc.stlab.rocksmap.transformer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

public class LongRocksTransformerByteBuffer implements RocksTransformer<Long> {

	public byte[] transform(Long value) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(value);
		return buffer.array();
	}

	@Override
	public Long transform(byte[] value) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		byte[] longBytes = Arrays.copyOfRange(value, 0, Long.BYTES);
		buffer.put(longBytes);
		buffer.flip();
		return buffer.getLong();
	}

	@Override
	public Collection<Long> transformCollection(byte[] value) {
		throw new UnsupportedOperationException();
	}

}
