package it.cnr.istc.stlab.rocksmap.transformer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;

import it.unimi.dsi.fastutil.io.BinIO;

public class ObjectRocksOnlineTransformer<T> implements RocksTransformer<T> {

	@SuppressWarnings("unchecked")
	public T transform(byte[] value) {
		try {
			return (T) BinIO.loadObject(new ByteArrayInputStream(value));
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public byte[] transform(T value) {
		ByteArrayOutputStream baos_obj = new ByteArrayOutputStream();
		try {
			BinIO.storeObject(value, baos_obj);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return baos_obj.toByteArray();
	}

	public byte[] transformCollection(Collection<T> value) {
		throw new UnsupportedOperationException();
	}

	public Collection<T> transformCollection(byte[] value) {
		throw new UnsupportedOperationException();
	}

}
