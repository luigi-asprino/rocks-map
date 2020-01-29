package it.cnr.istc.stlab.rocksmap;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionPriority;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;
import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public abstract class RocksDBWrapper<K, V> {

	static {
		RocksDB.loadLibrary();
	}

	protected static Logger logger = LoggerFactory.getLogger(RocksDBWrapper.class);

	protected RocksDB db;
	protected RocksTransformer<K> keyTransformer;
	protected RocksTransformer<V> valueTransformer;
	protected String rocksDBPath;
	protected boolean existed = false;

	protected static final char SEPARATOR_DUMP = '\t';

	public RocksDBWrapper(String rocksDBPath, RocksTransformer<K> keyTransformer, RocksTransformer<V> valueTransformer)
			throws RocksDBException {

		this.keyTransformer = keyTransformer;
		this.valueTransformer = valueTransformer;
		this.rocksDBPath = rocksDBPath;
		File f = new File(rocksDBPath);
		existed = f.exists();

		// Derived from
		// https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning

		Options options = new Options();
		BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
		// table_options.block_size = 16 * 1024;
		tableOptions.setBlockSize(16 * 1024);
		// table_options.cache_index_and_filter_blocks = true;
		tableOptions.setCacheIndexAndFilterBlocks(true);
		// table_options.pin_l0_filter_and_index_blocks_in_cache = true;
		tableOptions.setPinL0FilterAndIndexBlocksInCache(true);
		// https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#block-cache-size
		tableOptions.setBlockCache(new LRUCache(1 * SizeUnit.GB));
		//@f:off
		options.setCreateIfMissing(true)
			.setWriteBufferSize(512 * SizeUnit.MB)
			.setMaxWriteBufferNumber(2)
			.setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
			// table options
			.setTableFormatConfig(tableOptions)
			// cf_options.level_compaction_dynamic_level_bytes = true;
			.setLevelCompactionDynamicLevelBytes(true)
			// options.max_background_compactions = 4;
			.setMaxBackgroundCompactions(4)
			// options.max_background_flushes = 2;
			.setMaxBackgroundFlushes(2)
			// options.bytes_per_sync = 1048576;
			.setBytesPerSync(1048576)
			// options.compaction_pri = kMinOverlappingRatio;
			.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
		//@f:on
		f.mkdirs();
		db = RocksDB.open(options, rocksDBPath);
	}

	public String getRocksDBPath() {
		return this.rocksDBPath;
	}

	public void flush() throws RocksDBException {
		FlushOptions fo = new FlushOptions();
		fo.setWaitForFlush(true);
		db.flush(fo);
		fo.close();
	}

	public boolean containsKey(final Object key) {
		@SuppressWarnings("unchecked")
		final K k = (K) key;
		try {
			final byte[] v = db.get(keyTransformer.transform(k));
			return v != null;
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void clear() {
		ReadOptions ro = new ReadOptions();
		ro.setFillCache(false);
		RocksIterator ri = db.newIterator(ro);
		ri.seekToFirst();
		final byte[] first = ri.key();
		ri.seekToLast();
		final byte[] last = ri.key();
		ri.close();
		try {
			db.deleteRange(first, last);
			db.delete(last);
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
	}

	public void deleteRange(final byte[] first, final byte[] lastInclusive) {
		try {
			db.deleteRange(first, lastInclusive);
			db.delete(lastInclusive);
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
	}

	public Set<K> keySet() {
		Set<K> keys = new HashSet<>();
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		while (ri.isValid()) {
			byte[] key = ri.key();
			keys.add(keyTransformer.transform(key));
			ri.next();
		}
		ri.close();
		return keys;
	}

	public void toFile() throws IOException {
		logger.info("Dumping " + rocksDBPath + "/dump.tsv");
		CSVWriter csvw = new CSVWriter(new FileWriter(new File(rocksDBPath + "/dump.tsv")), SEPARATOR_DUMP,
				CSVWriter.NO_QUOTE_CHARACTER);
		logger.debug("Creating Iterator");
		RocksIterator ri = db.newIterator();
		logger.debug("Iterator created");
		ri.seekToFirst();
		while (ri.isValid()) {
			byte[] key = ri.key();
			byte[] val = ri.value();
			csvw.writeNext(new String[] { new String(key), new String(val) });
			ri.next();
		}
		csvw.close();
		ri.close();
		logger.info("Dumped");
	}

	public void close() {
		logger.info("Closing {}", this.rocksDBPath);
		db.close();
	}

	public long sizeLong() {
		long l = 0;
		try {
			l = db.getLongProperty("rocksdb.estimate-num-keys");
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return l;
	}

	public V removeKey(K key) {
		try {
			final byte[] k = keyTransformer.transform(key);
			final byte[] value = db.get(k);

			if (value != null) {
				logger.debug("Deleting {}", key.toString());
				db.delete(new WriteOptions(), k);
				logger.debug("Has Key {}", containsKey(key));
				return valueTransformer.transform(value);
			}
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Iterator<Entry<K, V>> entryIterator() {
		ReadOptions ro = new ReadOptions();
		ro.setFillCache(false);
		RocksIterator ri = db.newIterator(ro);
		ri.seekToFirst();
		Iterator<Entry<K, V>> result = new Iterator<Map.Entry<K, V>>() {

			@Override
			public boolean hasNext() {
				if (!ri.isValid()) {
					ri.close();
					ro.close();
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
			ro.close();
		}
		return result;
	}

	public Iterator<V> valueIterator() {
		ReadOptions ro = new ReadOptions();
		ro.setFillCache(false);
		RocksIterator ri = db.newIterator(ro);
		ri.seekToFirst();
		Iterator<V> result = new Iterator<V>() {

			@Override
			public boolean hasNext() {
				if (!ri.isValid()) {
					ri.close();
					ro.close();
					return false;
				}
				return true;
			}

			@Override
			public V next() {
				byte[] val = ri.value();
				ri.next();
				return valueTransformer.transform(val);
			}
		};
		if (!ri.isValid()) {
			ri.close();
			ro.close();
		}
		return result;
	}

	public Iterator<K> keyIterator() {
		ReadOptions ro = new ReadOptions();
		ro.setFillCache(false);
		RocksIterator ri = db.newIterator(ro);
		ri.seekToFirst();
		Iterator<K> result = new Iterator<K>() {

			@Override
			public boolean hasNext() {
				if (!ri.isValid()) {
					ri.close();
					ro.close();
					return false;
				}
				return true;
			}

			@Override
			public K next() {
				byte[] key = ri.key();
				ri.next();
				return keyTransformer.transform(key);
			}
		};
		if (!ri.isValid()) {
			ri.close();
			ro.close();
		}
		return result;
	}

	public void print() {
		Iterator<Entry<K, V>> it = entryIterator();
		while (it.hasNext()) {
			Entry<K, V> e = it.next();
			System.out.println(e.getKey().toString() + " -> " + e.getValue().toString());
		}
	}

}
