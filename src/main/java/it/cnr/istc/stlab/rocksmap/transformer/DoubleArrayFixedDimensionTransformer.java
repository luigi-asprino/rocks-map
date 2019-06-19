package it.cnr.istc.stlab.rocksmap.transformer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

import it.unimi.dsi.fastutil.io.BinIO;

public class DoubleArrayFixedDimensionTransformer implements RocksTransformer<double[]> {

	private int dimension;

	public DoubleArrayFixedDimensionTransformer(int dimension) {
		this.dimension = dimension;
	}

	@Override
	public double[] transform(byte[] value) {
		ByteArrayInputStream bais = new ByteArrayInputStream(value);
		DataInputStream dis = new DataInputStream(bais);
		double[] r = new double[dimension];
		try {
			BinIO.loadDoubles(dis, r);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return r;
	}

	public byte[] transform(double[] value) {
		if (value.length != dimension) {
			throw new RuntimeException("Wrong dimension!");
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			BinIO.storeDoubles(value, dos);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return baos.toByteArray();
	}

	public byte[] transformCollection(Collection<double[]> value) {
		throw new RuntimeException("Unsupported method!");
	}

	@Override
	public Collection<double[]> transformCollection(byte[] value) {
		throw new RuntimeException("Unsupported method!");
	}

}
