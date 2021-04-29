# Berkeley DB

High-performance embeddable database providing key-value storage.

## Performance bug report

I am using your program to store 500K entries, but it is taking quite some time to execute (about 61 seconds). This is
the configuration that I used:

* `ADLER32_CHUNK_SIZE = 1_000`
* `CACHE_MODE = EVICT_LN`
* `CHECKPOINTER_BYTES_INTERVAL = 20_000_000`
* `DEFERRED_WRITE = false`
* `DUPLICATES = true`
* `ENV_BACKGROUND_READ_LIMIT = 0`
* `ENV_IS_LOCKING = true`
* `ENV_SHARED_CACHE = true`
* `FLUSH_REQUIRED = false`
* `JE_DURABILITY = COMMIT_WRITE_NO_SYNC`
* `JE_FILE_LEVEL = INFO`
* `KEY_PREFIXING = false`
* `LATCH_TIMEOUT = 25 ms`
* `LOCK_DEADLOCK_DETECT = true`
* `LOCK_DEADLOCK_DETECT_DELAY = 10 sec`
* `MAX_MEMORY = 1_000_000`
* `NODE_MAX_ENTRIES = 512`
* `OFFHEAP_EVICT_BYTES = 102_400`
* `OVERRIDE_BTREE_COMPARATOR = false`
* `PROVISIONAL = false`
* `REPLICATED = false`
* `RUN_CLEANER = true`
* `RUN_EVICTOR = false`
* `RUN_OFFHEAP_EVICTOR = false`
* `RUN_VERIFIER = true`
* `SEQUENTIAL = false`
* `TEMPORARY = true`
* `TRANSACTIONS = true`
* `TXN_SERIALIZABLE_ISOLATION = true`
* `VERIFY_DATA_RECORDS = true`

Could you please take a look at why the program is taking so long?

## Task

**Address the bug report**. Specifically, **answer the question** in the bug report **"why is the system taking so long
to execute?"**?.

## Docs

### ADLER32_CHUNK_SIZE

Setting this parameter will cause JE to pass chunks of the log record to the checksumming class so that the GC does not
block.
0 means do not chunk.
Default = 0.

### CACHE_MODE
Modes that can be specified for control over caching of records in the JE in-memory cache.
Default = UNCHANGED

### CHECKPOINTER_BYTES_INTERVAL
Ask the checkpointer to run every time we write this many bytes to the log.
Default = 20000000.

### DEFERRED_WRITE
Open database as deffered-write.
Default = false;

### DUPLICATES
Configures the database to support records with duplicate keys.
Default = false.

### ENV_BACKGROUND_READ_LIMIT
The maximum number of read operations performed by JE background activities (e.g., cleaning) before sleeping to ensure that application threads can perform I/O.
If zero (the default) then no limitation on I/O is enforced.
Default = 0.

### ENV_IS_LOCKING
Configures the database environment for no locking.
Default = true.

### ENV_SHARED_CACHE
Whether to use the shared cache.
Default = false.

### FLUSH_REQUIRED
Whether the log buffer(s) must be written to the file system.
Default = false.

### JE_DURABILITY
Durability defines the overall durability characteristics associated with a transaction.
Default = COMMIT_NO_SYNC.

### JE_FILE_LEVEL
The level for JE FileHandler.
Default = OFF.

### KEY_PREFIXING 
Configure the database to support key prefixing.
Default = false;

### LATCH_TIMEOUT
The timeout for detecting internal latch timeouts, so that deadlocks can be detected.
Default = 5 ms.

### LOCK_DEADLOCK_DETECT
Whether to perform deadlock detection when a lock conflict occurs.
Default = false.

### LOCK_DEADLOCK_DETECT_DELAY
The delay after a lock conflict, before performing deadlock detection.
Default = 0 sec.

### MAX_MEMORY
Configures the JE main cache size in bytes.
Default = 1000000.

### NODE_MAX_ENTRIES
The maximum number of entries in an internal btree node.
Default = 128.

### OFFHEAP_EVICT_BYTES
The off-heap evictor will attempt to keep the max memory usage this number of bytes.
Default = 51_200.

### OVERRIDE_BTREE_COMPARATOR
Whether to override the btree comparator.
Default = false.

### PROVISIONAL
Whether the logged entry should be processed during recovery.
Default = false.

### REPLICATED
Configures a database to be replicated or non-replicated.
Default = false.

### RUN_CLEANER
Whether to run the cleaner in a separate thread.
Default = false.

### RUN_EVICTOR
Whether to run the evictor in a separate thread.
Default = false.

### RUN_OFFHEAP_EVICTOR
Whether to run the off-heap evictor in separate threads.
Default = false.

### RUN_VERIFIER
Whether to run the background verifier.
Default = false.

### SEQUENTIAL
Whether to write sequential data.
Default = false.

### TEMPORARY
Sets the temporary database option.
Temporary databases operate internally in deferred-write mode to provide reduced disk I/O and increased concurrency.
Default = false.

### TRANSACTIONS
Encloses the database opeartions within a transaction.
Default = false.

### TXN_SERIALIZABLE_ISOLATION
Configures all transactions for this environment to have Serializable (Degree 3) isolation.
Default = false.

### VERIFY_DATA_RECORDS
Whether to verify data records during Btree verification.
Default = false.
