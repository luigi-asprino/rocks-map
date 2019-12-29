package it.cnr.istc.stlab.rocksmap;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public class RocksMultiMap<K, V> extends RocksDBWrapper<K, V> implements Multimap<K, V>, Closeable {

	protected static Logger logger = LoggerFactory.getLogger(RocksMultiMap.class);

	public RocksMultiMap(String rocksDBPath, RocksTransformer<K> keyTransformer, RocksTransformer<V> valueTransformer)
			throws RocksDBException {
		super(rocksDBPath, keyTransformer, valueTransformer);
	}

	@Override
	public int size() {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isEmpty() {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsValue(Object value) {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsEntry(Object key, Object value) {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean putAll(Multimap<? extends K, ? extends V> multimap) {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<V> replaceValues(K key, Iterable<? extends V> values) {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object key, Object value) {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public Multiset<K> keys() {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<Entry<K, V>> entries() {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<K, Collection<V>> asMap() {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean put(K key, V value) {
		Collection<V> value_collection = null;
		if (containsKey(key)) {
			value_collection = get(key);
		}

		if (value_collection == null) {
			value_collection = new HashSet<>();
		}

		value_collection.add(value);

		try {
			super.db.put(super.keyTransformer.transform(key),
					super.valueTransformer.transformCollection(value_collection));
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public boolean putAll(K key, Iterable<? extends V> values) {
		Set<V> newValues = new HashSet<>();
		values.forEach(v -> {
			newValues.add(v);
		});
		try {
			db.put(keyTransformer.transform(key), valueTransformer.transformCollection(newValues));
			return true;
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Collection<V> removeAll(Object key) {
		@SuppressWarnings("unchecked")
		K keyk = (K) key;
		Collection<V> values = get(keyk);
		byte[] keyBytes = keyTransformer.transform(keyk);
		try {
			db.delete(keyBytes);
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return values;
	}

	@Override
	public Collection<V> get(K key) {
		Collection<V> result = null;
		try {
			byte[] r = db.get(keyTransformer.transform((K) key));
			if (r == null) {
				return null;
			}
			result = valueTransformer.transformCollection(r);
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public Collection<V> values() {
		Set<V> values = new HashSet<>();
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		while (ri.isValid()) {
			byte[] value = ri.value();
			values.addAll(valueTransformer.transformCollection(value));
			ri.next();
		}
		ri.close();
		return values;
	}

	public void putMap(Map<K, Collection<V>> map) {
		map.forEach((k, v) -> {
			this.putAll(k, v);
		});
	}

	public Iterator<Entry<K, Collection<V>>> iterator() {
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		Iterator<Entry<K, Collection<V>>> result = new Iterator<Map.Entry<K, Collection<V>>>() {

			@Override
			public boolean hasNext() {
				if (!ri.isValid()) {
					ri.close();
					return false;
				}
				return true;
			}

			@Override
			public Entry<K, Collection<V>> next() {
				byte[] key = ri.key();
				byte[] val = ri.value();
				ri.next();
				Entry<K, Collection<V>> entry = new Entry<K, Collection<V>>() {

					@Override
					public K getKey() {
						return keyTransformer.transform(key);
					}

					@Override
					public Collection<V> getValue() {
						return valueTransformer.transformCollection(val);
					}

					@Override
					public Collection<V> setValue(Collection<V> value) {
						throw new UnsupportedOperationException();
					}
				};

				return entry;
			}

		};
		return result;
	}

}
