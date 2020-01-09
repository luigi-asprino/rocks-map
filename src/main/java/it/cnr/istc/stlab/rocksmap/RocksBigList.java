package it.cnr.istc.stlab.rocksmap;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformer;
import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;
import it.unimi.dsi.fastutil.BigList;
import it.unimi.dsi.fastutil.BigListIterator;
import it.unimi.dsi.fastutil.BigSwapper;
import it.unimi.dsi.fastutil.longs.LongComparator;

public class RocksBigList<K> extends RocksDBWrapper<Long, K> implements BigList<K>, Collection<K>, BigSwapper {

	private static Logger logger = LoggerFactory.getLogger(RocksBigList.class);
	private long size = 0;

	public RocksBigList(String rocksDBPath, RocksTransformer<K> valueTransformer) throws RocksDBException {
		super(rocksDBPath, new LongRocksTransformer(), valueTransformer);
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<K> iterator() {
		return super.valueIterator();
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean add(K e) {
		try {
			db.put(keyTransformer.transform(sizeLong()), valueTransformer.transform(e));
			size++;
			return true;
		} catch (RocksDBException e1) {
			e1.printStackTrace();
		}

		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends K> c) {
		c.forEach(e -> {
			add(e);
		});
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		AtomicBoolean ab = new AtomicBoolean(false);
		c.forEach(e -> {
			ab.set(remove(e));
		});
		return ab.get();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		super.clear();
		size = 0;
	}

	@Override
	public long size64() {
		return sizeLong();
	}

	@Override
	public K get(long index) {
		rangeCheck(index);
		try {
			byte[] elemToReturn = db.get(keyTransformer.transform(index));
			if (elemToReturn == null)
				return null;
			K objectTOReturn = valueTransformer.transform(elemToReturn);
			return objectTOReturn;
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public K remove(long index) {
		rangeCheck(index);
		logger.trace("Size {}", sizeLong());
		long initSize = sizeLong();

		try {
			K result = valueTransformer.transform(db.get(keyTransformer.transform(index)));
			for (long i = index; i < initSize - 1; i++) {
				logger.trace("Moving {} to {}", i + 1, i);
				db.put(keyTransformer.transform(i), db.get(keyTransformer.transform(i + 1)));
			}
			size--;
			super.removeKey(initSize-1);
			return result;
		} catch (RocksDBException e1) {
			e1.printStackTrace();
		}

		return null;
	}

	private void rangeCheck(long index) {
		if (index >= sizeLong())
			throw new IndexOutOfBoundsException("Index out of bound " + index + "/" + sizeLong());
	}

	@Override
	public K set(long index, K element) {
		rangeCheck(index);
		try {
			K result = get(index);
			db.put(keyTransformer.transform(index), valueTransformer.transform(element));
			return result;
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void add(long index, K element) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();

	}

	@Override
	public void size(long size) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();

	}

	@Override
	public boolean addAll(long index, Collection<? extends K> c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public long indexOf(Object o) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public long lastIndexOf(Object o) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public BigListIterator<K> listIterator() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public BigListIterator<K> listIterator(long index) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public BigList<K> subList(long from, long to) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public long sizeLong() {
		return size;
	}

	@Override
	public void swap(long a, long b) {

//		logger.trace("SWAP {} and {}", a, b);
		K elemA = get(a);
		K elemB = get(b);
		try {
			db.put(keyTransformer.transform(a), valueTransformer.transform(elemB));
			db.put(keyTransformer.transform(b), valueTransformer.transform(elemA));
		} catch (RocksDBException e) {
			e.printStackTrace();
		}

	}

	public void sort(Comparator<K> c) {
		it.unimi.dsi.fastutil.BigArrays.mergeSort(0, size, new LongComparator() {
			@Override
			public int compare(long k1, long k2) {
				K e1 = get(k1);
				K e2 = get(k2);
				return c.compare(e1, e2);
			}
		}, this);
	}

}
