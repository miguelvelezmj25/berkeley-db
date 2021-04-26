package com.sleepycat.analysis;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.*;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.tree.IN;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class MeasureDiskOrderedScan {

  private static String ADLER32_CHUNK_SIZE;
  private static CacheMode CACHE_MODE;
  private static String CHECKPOINTER_BYTES_INTERVAL;
  private static boolean DUPLICATES;
  private static String ENV_BACKGROUND_READ_LIMIT;
  private static boolean ENV_IS_LOCKING;
  private static boolean ENV_SHARED_CACHE;
  private static Durability JE_DURABILITY;
  private static String JE_FILE_LEVEL;
  private static boolean LOCK_DEADLOCK_DETECT;
  private static String LOCK_DEADLOCK_DETECT_DELAY;
  private static long MAX_MEMORY;
  private static boolean REPLICATED;
  private static boolean SEQUENTIAL;
  private static boolean TEMPORARY;
  private static boolean TRANSACTIONS;
  private static boolean TXN_SERIALIZABLE_ISOLATION;

  private final Action action = Action.Populate;
  private final boolean keysOnly = false;
  private final boolean preload = false;
  private final boolean sequentialWrites;
  private final int keySize = 10;
  private final int nRecords = 500_000;
  private final long internalMemoryLimit = 100L * 1000 * 1000;
  private final long jeCacheSize;
  private final long lsnBatchSize = Long.MAX_VALUE;
  private final Random random = new Random(10);

  private boolean dupDb = false;
  private Database db = null;
  private Environment env = null;
  private int dataSize = 1000;

  public MeasureDiskOrderedScan() {
    boolean dataSizeSpecified = false;
    this.sequentialWrites = SEQUENTIAL;
    this.jeCacheSize = MAX_MEMORY;

    if (lsnBatchSize != Long.MAX_VALUE && internalMemoryLimit != Long.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Only one of lsnBatchSize and internalMemoryLimit may be "
              + "specified (not equal to Long.MAX_VALUE)");
    }
    if (dupDb && !dataSizeSpecified) {
      dataSize = keySize;
    }
  }

  public static void main(String[] args) throws IOException {
    ADLER32_CHUNK_SIZE = "1000";
    CACHE_MODE = CacheMode.EVICT_LN;
    CHECKPOINTER_BYTES_INTERVAL = "20000000";
    DUPLICATES = true;
    ENV_BACKGROUND_READ_LIMIT = "0";
    ENV_IS_LOCKING = true;
    ENV_SHARED_CACHE = true;
    JE_DURABILITY = Durability.COMMIT_WRITE_NO_SYNC;
    JE_FILE_LEVEL = "INFO";
    LOCK_DEADLOCK_DETECT = true;
    LOCK_DEADLOCK_DETECT_DELAY = "10 sec";
    MAX_MEMORY = 1000000;
    REPLICATED = false;
    SEQUENTIAL = false;
    TEMPORARY = true;
    TRANSACTIONS = true;
    TXN_SERIALIZABLE_ISOLATION = true;

    File output = new File("./tmp");
    FileUtils.forceDelete(output);
    FileUtils.forceMkdir(output);
    new MeasureDiskOrderedScan().exec();

    System.out.println("LogManager.COUNT_LOG " + LogManager.COUNT_LOG);
    System.out.println("FileManager.COUNT_READ " + FileManager.COUNT_READ);
    System.out.println("FileManager.COUNT_WRITE " + FileManager.COUNT_WRITE);
    System.out.println("FileManager$LogEndFileDescriptor.COUNT_FORCE " + FileManager.COUNT_FORCE);
    System.out.println("IN.COUNT_FIND " + IN.COUNT_FIND);
    System.out.println("IN.COUNT_SERIALIZE " + IN.COUNT_SERIALIZE);
  }

  private static long maxMemory(boolean option) {
    long value = 1000L * 1000;

    if (option) {
      return value * 1000;
    }

    return value;
  }

  private static String jeFileLevel(boolean option) {
    if (option) {
      return "INFO";
    }

    return "OFF";
  }

  private static String lockDeadlockDetectDelay(boolean option) {
    if (option) {
      return "1 min";
    }

    return EnvironmentParams.LOCK_DEADLOCK_DETECT_DELAY.getDefault();
  }

  private static boolean envIsLocking(boolean option) {
    return true;
  }

  private static Durability jeDurability(boolean option) {
    if (option) {
      return Durability.COMMIT_SYNC;
    }

    return Durability.COMMIT_NO_SYNC;
  }

  private static String checkpointerBytesInterval(boolean option) {
    if (option) {
      return EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL.getMax();
    }

    return EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL.getDefault();
  }

  private static String adler32ChunkSize(boolean option) {
    if (option) {
      EnvironmentParams.ADLER32_CHUNK_SIZE.getMax();
    }

    return EnvironmentParams.ADLER32_CHUNK_SIZE.getDefault();
  }

  private static String envBackgroundReadLimit(boolean option) {
    if (option) {
      return EnvironmentParams.ENV_BACKGROUND_READ_LIMIT.getMax();
    }

    return EnvironmentParams.ENV_BACKGROUND_READ_LIMIT.getDefault();
  }

  private static CacheMode cacheMode(boolean option) {
    return option ? CacheMode.UNCHANGED : CacheMode.EVICT_LN;
  }

  public static void deleteFolder(File folder) {
    File[] files = folder.listFiles();
    if (files != null) { // some JVMs return null for empty dirs
      for (File f : files) {
        if (f.isDirectory()) {
          deleteFolder(f);
        } else {
          f.delete();
        }
      }
    }
    folder.delete();
  }

  private void exec() throws IOException {
    this.open();
    if (preload) {
      db.preload(null); /* LNs are not loaded. */
    }
    final double startTime = System.currentTimeMillis();
    switch (action) {
      case Populate:
        this.populate();
        break;
      case DirtyReadScan:
        this.dirtyReadScan();
        break;
      case DiskOrderedScan:
        this.diskOrderedScan();
        break;
      default:
        fail(action);
    }
    final double endTime = System.currentTimeMillis();
    final double totalSecs = (endTime - startTime) / 1000;
    final double throughput = nRecords / totalSecs;
    System.out.println("\nTotal seconds: " + totalSecs + " txn/sec: " + throughput);
    close();
  }

  private void open() throws IOException {
    final long minMemory =
        (internalMemoryLimit != Long.MAX_VALUE ? internalMemoryLimit : 0)
            + jeCacheSize
            + (jeCacheSize / 2);

    if (Runtime.getRuntime().maxMemory() < minMemory) {
      throw new IllegalArgumentException(
          "Must set heap size to at least internalMemoryLimit (if "
              + "specified) plus 1.5 X jeCacheSize: "
              + minMemory);
    }

    final boolean create = (action == Action.Populate);

    final EnvironmentConfig envConfig = new EnvironmentConfig();
    envConfig.setTransactional(true);
    envConfig.setAllowCreate(create);
    envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, String.valueOf(1000 * 1000 * 1000));

    /* Options */
    envConfig.setCacheSize(jeCacheSize);
    envConfig.setLocking(ENV_IS_LOCKING);
    envConfig.setSharedCache(ENV_SHARED_CACHE);
    envConfig.setTxnSerializableIsolation(TXN_SERIALIZABLE_ISOLATION);
    envConfig.setDurability(JE_DURABILITY);
    envConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL, JE_FILE_LEVEL);
    envConfig.setConfigParam(
        EnvironmentConfig.ENV_BACKGROUND_READ_LIMIT, ENV_BACKGROUND_READ_LIMIT);
    envConfig.setConfigParam(
        EnvironmentConfig.LOCK_DEADLOCK_DETECT, String.valueOf(LOCK_DEADLOCK_DETECT));
    envConfig.setConfigParam(EnvironmentConfig.ADLER32_CHUNK_SIZE, ADLER32_CHUNK_SIZE);
    envConfig.setConfigParam(
        EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, CHECKPOINTER_BYTES_INTERVAL);
    envConfig.setConfigParam(
        EnvironmentConfig.LOCK_DEADLOCK_DETECT_DELAY, LOCK_DEADLOCK_DETECT_DELAY);
    /* Options */

    /* Daemons interfere with cache size measurements. */
    envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR, "false");
    envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR, "false");
    envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
    envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
    envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
    envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER, "false");
    envConfig.setConfigParam(EnvironmentConfig.STATS_COLLECT, "false");
    /* Daemons interfere with cache size measurements. */

    String homeDir = "tmp";
    this.env = new Environment(new File(homeDir), envConfig);

    final DatabaseConfig dbConfig = new DatabaseConfig();
    dbConfig.setAllowCreate(create);
    dbConfig.setExclusiveCreate(create);

    /* Options */
    dbConfig.setSortedDuplicates(DUPLICATES);
    dbConfig.setTransactional(TRANSACTIONS);
    dbConfig.setReplicated(REPLICATED);
    dbConfig.setCacheMode(CACHE_MODE);
    dbConfig.setTemporary(TEMPORARY);
    /* Options */

    this.db = env.openDatabase(null, "foo", dbConfig);

    this.dupDb = dbConfig.getSortedDuplicates();
  }

  private void close() {
    db.close();
    env.close();
  }

  private void populate() {
    Put putType;
    if(this.dupDb) {
      putType = Put.DUP_DATA;
    }
    else {
      putType = Put.NO_OVERWRITE;
    }
    final DatabaseEntry key = new DatabaseEntry();
    final DatabaseEntry data = new DatabaseEntry();
    for (long i = 0; i < nRecords; i += 1) {
      if (sequentialWrites) {
        makeLongKey(key, i);
      } else {
        makeRandomKey(key);
      }
      makeData(data);
      OperationStatus status;
      /* Insert */
      if (this.dupDb) {
        status = this.db.putDupData(key, putType, data);
      } else {
        status = this.db.putNoOverwrite(key, putType, data);
      }
      if (status != OperationStatus.SUCCESS) {
        fail(status);
      }
      //      /* Update to create waste */
      //      status = db.put(null, key, data);
      //      if (status != OperationStatus.SUCCESS) {
      //        fail(status);
      //      }
    }
  }

  private void dirtyReadScan() {
    final DatabaseEntry key = new DatabaseEntry();
    final DatabaseEntry data = new DatabaseEntry();
    if (keysOnly) {
      data.setPartial(0, 0, true);
    }
    final Cursor cursor = db.openCursor(null, null);
    int nScanned = 0;
    while (cursor.getNext(key, data, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
      if (sequentialWrites) {
        checkLongKey(key, nScanned);
      } else {
        checkAnyKey(key);
      }
      if (!keysOnly) {
        checkData(data);
      }
      nScanned += 1;
    }
    cursor.close();
    checkEquals(nRecords, nScanned);
  }

  private void diskOrderedScan() {
    final DatabaseEntry key = new DatabaseEntry();
    final DatabaseEntry data = new DatabaseEntry();
    final DiskOrderedCursorConfig config = new DiskOrderedCursorConfig();
    config.setKeysOnly(keysOnly);
    config.setInternalMemoryLimit(internalMemoryLimit);
    config.setLSNBatchSize(lsnBatchSize);
    final DiskOrderedCursor cursor = db.openCursor(config);
    int nScanned = 0;
    while (cursor.getNext(key, data, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
      checkAnyKey(key);
      if (!keysOnly) {
        checkData(data);
      }
      nScanned += 1;
    }
    cursor.close();
    checkEquals(nRecords, nScanned);
  }

  private void makeLongKey(DatabaseEntry key, long value) {
    final byte[] bytes = new byte[keySize];
    final TupleOutput out = new TupleOutput(bytes);
    out.writeLong(value);
    key.setData(bytes);
  }

  private void checkLongKey(DatabaseEntry key, long value) {
    checkEquals(keySize, key.getSize());
    final TupleInput in = new TupleInput(key.getData());
    checkEquals(value, in.readLong());
  }

  private void makeRandomKey(DatabaseEntry key) {
    final byte[] bytes = new byte[keySize];
    random.nextBytes(bytes);
    key.setData(bytes);
  }

  private void checkAnyKey(DatabaseEntry key) {
    checkEquals(keySize, key.getSize());
  }

  private void makeData(DatabaseEntry data) {
    final byte[] bytes = new byte[dataSize];
    for (int i = 0; i < bytes.length; i += 1) {
      bytes[i] = (byte) i;
    }
    data.setData(bytes);
  }

  private void checkData(DatabaseEntry data) {
    checkEquals(dataSize, data.getSize());
    final byte[] bytes = data.getData();
    for (int i = 0; i < bytes.length; i += 1) {
      checkEquals(bytes[i], (byte) i);
    }
  }

  private void fail(Object msg) {
    throw new IllegalStateException(msg.toString());
  }

  private void check(boolean cond) {
    if (!cond) {
      fail("check failed, see stack");
    }
  }

  private void checkEquals(Object o1, Object o2) {
    if (o1 == null || o2 == null) {
      if (o1 != null || o2 != null) {
        fail("Only one is null; o1=" + o1 + " o2=" + o2);
      }
    }
    if (!o1.equals(o2)) {
      fail("Not equal; o1=" + o1 + " o2=" + o2);
    }
  }

  private void printArgs(String[] args) {
    System.out.print("Command line arguments:");
    for (String arg : args) {
      System.out.print(' ');
      System.out.print(arg);
    }
    System.out.println();
    System.out.println(
        "Effective arguments:"
            + " action="
            + action
            + " dupDb="
            + dupDb
            + " keysOnly="
            + keysOnly
            + " preload="
            + preload
            + " sequentialWrites="
            + sequentialWrites
            + " nRecords="
            + nRecords
            + " keySize="
            + keySize
            + " dataSize="
            + dataSize
            + " lsnBatchSize="
            + lsnBatchSize
            + " internalMemoryLimit="
            + internalMemoryLimit
            + " jeCacheSize="
            + jeCacheSize);
  }

  private enum Action {
    Populate,
    DirtyReadScan,
    DiskOrderedScan
  }
}
