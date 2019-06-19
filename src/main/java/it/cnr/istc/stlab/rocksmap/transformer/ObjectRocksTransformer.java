package it.cnr.istc.stlab.rocksmap.transformer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;

import it.unimi.dsi.fastutil.io.BinIO;

public class ObjectRocksTransformer implements RocksTransformer<Object> {

	@Override
	public Object transform(byte[] value) {
		try {
			return BinIO.loadObject(new ByteArrayInputStream(value));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<Object> transformCollection(byte[] value) {
		try {
			return (Collection<Object>) BinIO.loadObject(new ByteArrayInputStream(value));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public byte[] transform(Object value) {

		ByteArrayOutputStream baos_obj = new ByteArrayOutputStream();
		try {
			BinIO.storeObject(value, baos_obj);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos_obj.toByteArray();
	}

	public byte[] transformCollection(Collection<Object> value) {
		ByteArrayOutputStream baos_obj = new ByteArrayOutputStream();
		try {
			BinIO.storeObject(value, baos_obj);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos_obj.toByteArray();
	}

}
