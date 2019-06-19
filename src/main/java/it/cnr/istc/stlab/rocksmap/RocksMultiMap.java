package it.cnr.istc.stlab.rocksmap;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public class RocksMultiMap<K, V> implements Multimap<K, V>, Closeable {

	protected RocksDB db;
	protected RocksTransformer<K> keyTransformer;
	protected RocksTransformer<V> valueTransformer;
	protected String rocksDBPath;
	protected static Logger logger = LoggerFactory.getLogger(RocksMultiMap.class);

	private static final char SEPARATOR_DUMP = '\t';

	public RocksMultiMap(String rocksDBPath, RocksTransformer<K> keyTransformer, RocksTransformer<V> valueTransformer) throws RocksDBException {
		RocksDB.loadLibrary();
		this.keyTransformer = keyTransformer;
		this.valueTransformer = valueTransformer;
		this.rocksDBPath = rocksDBPath;
		File f = new File(rocksDBPath);
		logger.info("{} does not exist!", rocksDBPath);
		Options options = new Options();
		options.setCreateIfMissing(true);
		options.setIncreaseParallelism(8);
		f.mkdirs();
		db = RocksDB.open(options, rocksDBPath);

	}

	public RocksMultiMap(String rocksDBPath, RocksTransformer<K> keyTransformer, RocksTransformer<V> valueTransformer, String dumpFile) throws RocksDBException, IOException {
		RocksDB.loadLibrary();
		this.keyTransformer = keyTransformer;
		this.valueTransformer = valueTransformer;
		this.rocksDBPath = rocksDBPath;
		File f = new File(rocksDBPath);
		if (f.exists()) {
			logger.info("{}  exist!", rocksDBPath);
			Options options = new Options();
			options.setCreateIfMissing(true);
			new File(rocksDBPath).mkdirs();
			db = RocksDB.open(options, rocksDBPath);
			logger.info("DB exists appending dump!");
		} else {
			db = RocksDB.open(rocksDBPath);
		}
		populateRocksDBFromDump(dumpFile);
	}

	private void populateRocksDBFromDump(String dumpFile) throws IOException, RocksDBException {
		logger.info("Appending Dump to DB");
		CSVReader csvr = new CSVReader(new FileReader(new File(dumpFile)), SEPARATOR_DUMP, CSVWriter.NO_QUOTE_CHARACTER);
		String[] line;
		while ((line = csvr.readNext()) != null) {
			db.put(line[0].getBytes(), line[1].getBytes());
		}
		csvr.close();
	}

	public String getRocksDBPath() {
		return this.rocksDBPath;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isEmpty() {
		throw new UnsupportedOperationException();
		// TODO Auto-generated method stub
	}

	@Override
	public boolean containsKey(Object key) {
		@SuppressWarnings("unchecked")
		K k = (K) key;
		try {
			byte[] v = db.get(keyTransformer.transform(k));
			return v != null;
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsEntry(Object key, Object value) {
		// TODO Auto-generated method stub
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
			db.put(keyTransformer.transform(key), valueTransformer.transformCollection(value_collection));
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public boolean remove(Object key, Object value) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
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
	public boolean putAll(Multimap<? extends K, ? extends V> multimap) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<V> replaceValues(K key, Iterable<? extends V> values) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
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
	public void clear() {
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		byte[] first = ri.key();
		ri.seekToLast();
		byte[] last = ri.key();
		try {
			db.deleteRange(first, last);
			db.delete(last);
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		// while (ri.isValid()) {
		// byte[] key = ri.key();
		// try {
		// db.delete(key);
		// } catch (RocksDBException e) {
		// e.printStackTrace();
		// }
		// ri.next();
		// }
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
	public Set<K> keySet() {
		Set<K> keys = new HashSet<>();
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		while (ri.isValid()) {
			byte[] key = ri.key();
			keys.add(keyTransformer.transform(key));
			ri.next();
		}
		return keys;
	}

	@Override
	public Multiset<K> keys() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
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
		return values;
	}

	@Override
	public Collection<Entry<K, V>> entries() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<K, Collection<V>> asMap() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void close() {
		logger.info("Closing {}... skipped", rocksDBPath);
		// db.getDefaultColumnFamily().close();
		// db.close();
	}

	public void toFile() throws IOException {
		logger.info("Dumping " + rocksDBPath + "/dump.tsv");
		CSVWriter csvw = new CSVWriter(new FileWriter(new File(rocksDBPath + "/dump.tsv")), SEPARATOR_DUMP, CSVWriter.NO_QUOTE_CHARACTER);
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		while (ri.isValid()) {
			byte[] key = ri.key();
			byte[] val = ri.value();
			csvw.writeNext(new String[] { new String(key), new String(val) });
			ri.next();
		}
		csvw.close();
		logger.info("Dumped");
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
				return ri.isValid();
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
