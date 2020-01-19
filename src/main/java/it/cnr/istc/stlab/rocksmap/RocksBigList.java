package it.cnr.istc.stlab.rocksmap;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.cnr.istc.stlab.rocksmap.transformer.LongRocksTransformerByteBuffer;
import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;
import it.cnr.istc.stlab.rocksmap.utils.ParallelQuickSort;
import it.unimi.dsi.fastutil.BigList;
import it.unimi.dsi.fastutil.BigListIterator;
import it.unimi.dsi.fastutil.BigSwapper;
import it.unimi.dsi.fastutil.longs.LongComparator;

public class RocksBigList<K> extends RocksDBWrapper<Long, K> implements BigList<K>, Collection<K>, BigSwapper {

	private static Logger logger = LoggerFactory.getLogger(RocksBigList.class);
	private AtomicLong size = new AtomicLong(0);

	public RocksBigList(String rocksDBPath, RocksTransformer<K> valueTransformer) throws RocksDBException {
		super(rocksDBPath, new LongRocksTransformerByteBuffer(), valueTransformer);
	}

	@Override
	public boolean isEmpty() {
		return size.longValue() == 0;
	}

	@Override
	public Iterator<K> iterator() {
		return super.valueIterator();
	}

	public Spliterator<K> spliterator() {
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		return new RocksSpliterator(ri, 0L, size64());
	}

	class RocksSpliterator implements Spliterator<K> {
		private RocksIterator ri;
		private Long endExclusive, start;

		RocksSpliterator(RocksIterator ri, Long start, Long endExclusive) {
			this.ri = ri;
			this.start = start;
			ri.seek(keyTransformer.transform(start));
			this.endExclusive = endExclusive;
		}

		@Override
		public boolean tryAdvance(Consumer<? super K> action) {
			if (!ri.isValid() || keyTransformer.transform(ri.key()) >= endExclusive) {
				ri.close();
				return false;
			}
			final byte[] val = ri.value();
			action.accept(valueTransformer.transform(val));
			ri.next();
			return true;
		}

		@Override
		public Spliterator<K> trySplit() {
			final Long currentPos = keyTransformer.transform(ri.key());
			final Long newIteratorStart = (currentPos - endExclusive) / 2;
			final Long newIteratorEnd = endExclusive;
			endExclusive = newIteratorStart;
			return new RocksSpliterator(db.newIterator(), newIteratorStart, newIteratorEnd);
		}

		@Override
		public long estimateSize() {
			return endExclusive - start;
		}

		@Override
		public int characteristics() {
			return SIZED;
		}

	}

	@Override
	public synchronized boolean add(K e) {
		try {
			db.put(keyTransformer.transform(size.getAndIncrement()), valueTransformer.transform(e));
			return true;
		} catch (RocksDBException e1) {
			e1.printStackTrace();
		}

		return false;
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
	public void clear() {
		super.clear();
		size = new AtomicLong(0L);
	}

	@Override
	public long size64() {
		return sizeLong();
	}

	@Override
	public K get(final long index) {
		rangeCheck(index);
		try {
			final byte[] elemToReturn = db.get(keyTransformer.transform(index));
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
	public synchronized K remove(long index) {
		rangeCheck(index);
		logger.trace("Size {}", sizeLong());
		long initSize = size.longValue();
		try {
			K result = valueTransformer.transform(db.get(keyTransformer.transform(index)));
			LongStream.range(index, initSize - 1).parallel().forEach(i -> {
				try {
					db.put(keyTransformer.transform(i), db.get(keyTransformer.transform(i + 1)));
				} catch (RocksDBException e) {
					e.printStackTrace();
				}
			});
//			for (long i = index; i < initSize - 1; i++) {
//				logger.trace("Moving {} to {}", i + 1, i);
//				db.put(keyTransformer.transform(i), db.get(keyTransformer.transform(i + 1)));
//			}
			size.decrementAndGet();
			super.removeKey(initSize - 1);
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

	public long sizeLong() {
		return size.longValue();
	}

	@Override
	public void swap(final long a, final long b) {

//		logger.trace("SWAP {} and {}", a, b);
//		K elemA = get(a);
//		K elemB = get(b);
		final byte[] keyA = keyTransformer.transform(a);
		final byte[] keyB = keyTransformer.transform(b);
		try {
//			db.put(keyTransformer.transform(a), valueTransformer.transform(elemB));
//			db.put(keyTransformer.transform(b), valueTransformer.transform(elemA));
			final byte[] valA = db.get(keyA);
			final byte[] valB = db.get(keyB);
			db.put(keyA, valB);
			db.put(keyB, valA);
		} catch (RocksDBException e) {
			e.printStackTrace();
		}

	}
// TODO
//	public Stream<K> stream() {
//		return null;
//	}

	public void sort(Comparator<K> c) {
		ParallelQuickSort.quickSort(0, size.longValue(), new LongComparator() {
			@Override
			public int compare(long k1, long k2) {
				K e1 = get(k1);
				K e2 = get(k2);
				return c.compare(e1, e2);
			}
		}, this);
	}

	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
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
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
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

}
