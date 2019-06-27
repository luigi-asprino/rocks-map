package it.cnr.istc.stlab.rocksmap.transformer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

import it.unimi.dsi.fastutil.io.BinIO;

public class StringRocksTransformer implements RocksTransformer<String> {

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
		ByteArrayInputStream bais = new ByteArrayInputStream(value);
		DataInputStream dis = new DataInputStream(bais);
		Collection<String> r = null;
		try {
			r = (Collection<String>) BinIO.loadObject(dis);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return r;
	}

	@Override
	public byte[] transformCollection(Collection<String> value) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			BinIO.storeObject(value, dos);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return baos.toByteArray();
	}

}
