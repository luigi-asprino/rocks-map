package it.cnr.istc.stlab.rocksmap;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public class RocksMap<K, V> extends RocksDBWrapper<K, V> implements Map<K, V>, Closeable {

	public RocksMap(String rocksDBPath, RocksTransformer<K> keyTransformer, RocksTransformer<V> valueTransformer)
			throws RocksDBException {
		super(rocksDBPath, keyTransformer, valueTransformer);
	}

	@Override
	public int size() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isEmpty() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsValue(Object value) {
		// TODO
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		V result = null;
		try {
			byte[] r = db.get(keyTransformer.transform((K) key));
			if (r == null)
				return null;
			result = valueTransformer.transform(r);
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public V put(K key, V value) {
		V old_value = null;
		if (containsKey(key)) {
			old_value = get(key);
		}
		try {
			db.put(keyTransformer.transform(key), valueTransformer.transform(value));
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return old_value;
	}

	@Override
	public V remove(Object key) {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		m.forEach((k, v) -> {
			this.put(k, v);
		});
	}

	@Override
	public Collection<V> values() {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		// TODO
		throw new UnsupportedOperationException();
	}

	public Iterator<Entry<K, V>> iterator() {
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		Iterator<Entry<K, V>> result = new Iterator<Map.Entry<K, V>>() {

			@Override
			public boolean hasNext() {
				if (!ri.isValid()) {
					ri.close();
					return false;
				}
				return true;
			}

			@Override
			public Entry<K, V> next() {
				byte[] val = ri.value();
				byte[] key = ri.key();
				Entry<K, V> entry = new Entry<K, V>() {

					@Override
					public K getKey() {
						return keyTransformer.transform(key);

					}

					@Override
					public V getValue() {
						return valueTransformer.transform(val);
					}

					@Override
					public V setValue(V value) {
						throw new UnsupportedOperationException();
					}
				};
				ri.next();
				return entry;
			}
		};
		if (!ri.isValid()) {
			ri.close();
		}
		return result;
	}

}
