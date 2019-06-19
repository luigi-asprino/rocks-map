package it.cnr.istc.stlab.rocksmap.transformer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

public class DoubleRocksTransformer implements RocksTransformer<Double> {

	@Override
	public Double transform(byte[] value) {
		ByteArrayInputStream bais_1 = new ByteArrayInputStream(value);
		DataInputStream dis_1 = new DataInputStream(bais_1);

		double d = Double.MIN_VALUE;
		try {
			d = dis_1.readDouble();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return d;
	}

	@Override
	public Collection<Double> transformCollection(byte[] value) {
		throw new RuntimeException("Unsupported Method!");
	}

	public byte[] transform(Double value) {
		ByteArrayOutputStream baos_1 = new ByteArrayOutputStream();
		DataOutputStream dos_1 = new DataOutputStream(baos_1);
		try {
			dos_1.writeDouble(value);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return baos_1.toByteArray();
	}

	public byte[] transformCollection(Collection<Double> value) {
		throw new RuntimeException("Unsupported Method!");
	}

}
