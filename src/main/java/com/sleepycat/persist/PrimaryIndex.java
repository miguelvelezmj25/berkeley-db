/*-
 * Copyright (C) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */

package com.sleepycat.persist;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.compat.DbCompat;
import com.sleepycat.je.*;
import com.sleepycat.persist.impl.PersistEntityBinding;
import com.sleepycat.persist.impl.PersistKeyAssigner;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import java.util.Map;
import java.util.SortedMap;

/**
 * The primary index for an entity class and its primary key.
 *
 * <p>{@code PrimaryIndex} objects are thread-safe. Multiple threads may safely call the methods of
 * a shared {@code PrimaryIndex} object.
 *
 * <p>{@code PrimaryIndex} implements {@link EntityIndex} to map the primary key type (PK) to the
 * entity type (E).
 *
 * <p>The {@link Entity} annotation may be used to define an entity class and the {@link PrimaryKey}
 * annotation may be used to define a primary key as shown in the following example.
 *
 * <pre class="code">
 * {@literal @Entity}
 * class Employee {
 *
 *     {@literal @PrimaryKey}
 *     long id;
 *
 *     String name;
 *
 *     Employee(long id, String name) {
 *         this.id = id;
 *         this.name = name;
 *     }
 *
 *     private Employee() {} // For bindings
 * }</pre>
 *
 * <p>To obtain the {@code PrimaryIndex} for a given entity class, call {@link
 * EntityStore#getPrimaryIndex EntityStore.getPrimaryIndex}, passing the primary key class and the
 * entity class. For example:
 *
 * <pre class="code">
 * EntityStore store = new EntityStore(...);
 *
 * {@code PrimaryIndex<Long, Employee>} primaryIndex =
 *     store.getPrimaryIndex(Long.class, Employee.class);</pre>
 *
 * <p>Note that {@code Long.class} is passed as the primary key class, but the primary key field has
 * the primitive type {@code long}. When a primitive primary key field is used, the corresponding
 * primitive wrapper class is used to access the primary index. For more information on key field
 * types, see {@link PrimaryKey}.
 *
 * <p>The {@code PrimaryIndex} provides the primary storage and access methods for the instances of
 * a particular entity class. Entities are inserted and updated in the {@code PrimaryIndex} by
 * calling a method in the family of {@link #put} methods. The {@link #put} method will insert the
 * entity if no entity with the same primary key already exists. If an entity with the same primary
 * key does exist, it will update the entity and return the existing (old) entity. For example:
 *
 * <pre class="code">
 * Employee oldEntity;
 * oldEntity = primaryIndex.put(new Employee(1, "Jane Smith"));    // Inserts an entity
 * assert oldEntity == null;
 * oldEntity = primaryIndex.put(new Employee(2, "Joan Smith"));    // Inserts an entity
 * assert oldEntity == null;
 * oldEntity = primaryIndex.put(new Employee(2, "Joan M. Smith")); // Updates an entity
 * assert oldEntity != null;</pre>
 *
 * <p>The {@link #putNoReturn} method can be used to avoid the overhead of returning the existing
 * entity, when the existing entity is not important to the application. The return type of {@link
 * #putNoReturn} is void. For example:
 *
 * <pre class="code">
 * primaryIndex.putNoReturn(new Employee(1, "Jane Smith"));    // Inserts an entity
 * primaryIndex.putNoReturn(new Employee(2, "Joan Smith"));    // Inserts an entity
 * primaryIndex.putNoReturn(new Employee(2, "Joan M. Smith")); // Updates an entity</pre>
 *
 * <p>The {@link #putNoOverwrite} method can be used to ensure that an existing entity is not
 * overwritten. {@link #putNoOverwrite} returns true if the entity was inserted, or false if an
 * existing entity exists and no action was taken. For example:
 *
 * <pre class="code">
 * boolean inserted;
 * inserted = primaryIndex.putNoOverwrite(new Employee(1, "Jane Smith"));    // Inserts an entity
 * assert inserted;
 * inserted = primaryIndex.putNoOverwrite(new Employee(2, "Joan Smith"));    // Inserts an entity
 * assert inserted;
 * inserted = primaryIndex.putNoOverwrite(new Employee(2, "Joan M. Smith")); // <strong>No action was taken!</strong>
 * assert !inserted;</pre>
 *
 * <p>Primary key values must be unique, in other words, each instance of a given entity class must
 * have a distinct primary key value. Rather than assigning the unique primary key values yourself,
 * a <em>sequence</em> can be used to assign sequential integer values automatically, starting with
 * the value 1 (one). A sequence is defined using the {@link PrimaryKey#sequence} annotation
 * property. For example:
 *
 * <pre class="code">
 * {@literal @Entity}
 * class Employee {
 *
 *     {@literal @PrimaryKey(sequence="ID")}
 *     long id;
 *
 *     String name;
 *
 *     Employee(String name) {
 *         this.name = name;
 *     }
 *
 *     private Employee() {} // For bindings
 * }</pre>
 *
 * <p>The name of the sequence used above is "ID". Any name can be used. If the same sequence name
 * is used in more than one entity class, the sequence will be shared by those classes, in other
 * words, a single sequence of integers will be used for all instances of those classes. See {@link
 * PrimaryKey#sequence} for more information.
 *
 * <p>Any method in the family of {@link #put} methods may be used to insert entities where the
 * primary key is assigned from a sequence. When the {@link #put} method returns, the primary key
 * field of the entity object will be set to the assigned key value. For example:
 *
 * <pre class="code">
 * Employee employee;
 * employee = new Employee("Jane Smith");
 * primaryIndex.putNoReturn(employee);    // Inserts an entity
 * assert employee.id == 1;
 * employee = new Employee("Joan Smith");
 * primaryIndex.putNoReturn(employee);    // Inserts an entity
 * assert employee.id == 2;</pre>
 *
 * <p>This begs the question: How do you update an existing entity, without assigning a new primary
 * key? The answer is that the {@link #put} methods will only assign a new key from the sequence if
 * the primary key field is zero or null (for reference types). If an entity with a non-zero and
 * non-null key field is passed to a {@link #put} method, any existing entity with that primary key
 * value will be updated. For example:
 *
 * <pre class="code">
 * Employee employee;
 * employee = new Employee("Jane Smith");
 * primaryIndex.putNoReturn(employee);    // Inserts an entity
 * assert employee.id == 1;
 * employee = new Employee("Joan Smith");
 * primaryIndex.putNoReturn(employee);    // Inserts an entity
 * assert employee.id == 2;
 * employee.name = "Joan M. Smith";
 * primaryIndex.putNoReturn(employee);    // Updates an existing entity
 * assert employee.id == 2;</pre>
 *
 * <p>Since {@code PrimaryIndex} implements the {@link EntityIndex} interface, it shares the common
 * index methods for retrieving and deleting entities, opening cursors and using transactions. See
 * {@link EntityIndex} for more information on these topics.
 *
 * <p>Note that when using an index, keys and values are stored and retrieved by value not by
 * reference. In other words, if an entity object is stored and then retrieved, or retrieved twice,
 * each object will be a separate instance. For example, in the code below the assertion will always
 * fail.
 *
 * <pre class="code">
 * MyKey key = ...;
 * MyEntity entity1 = new MyEntity(key, ...);
 * index.put(entity1);
 * MyEntity entity2 = index.get(key);
 * assert entity1 == entity2; // always fails!
 * </pre>
 *
 * @author Mark Hayes
 */
public class PrimaryIndex<PK, E> extends BasicIndex<PK, E> {

  private final Class<E> entityClass;
  private final EntityBinding<E> entityBinding;
  private SortedMap<PK, E> map;
  private PersistKeyAssigner keyAssigner;

  /**
   * Creates a primary index without using an <code>EntityStore</code>.
   *
   * <p>This constructor is not normally needed and is provided for applications that wish to use
   * custom bindings along with the Direct Persistence Layer. Normally, {@link
   * EntityStore#getPrimaryIndex getPrimaryIndex} is used instead.
   *
   * <p>Note that when this constructor is used directly, primary keys cannot be automatically
   * assigned from a sequence. The key assignment feature requires knowledge of the primary key
   * field, which is only available if an <code>EntityStore</code> is used. Of course, primary keys
   * may be assigned from a sequence manually before calling the <code>put</code> methods in this
   * class.
   *
   * @param database the primary database.
   * @param keyClass the class of the primary key.
   * @param keyBinding the binding to be used for primary keys.
   * @param entityClass the class of the entities stored in this index.
   * @param entityBinding the binding to be used for entities.
   * @throws DatabaseException the base class for all BDB exceptions.
   */
  public PrimaryIndex(
      Database database,
      Class<PK> keyClass,
      EntryBinding<PK> keyBinding,
      Class<E> entityClass,
      EntityBinding<E> entityBinding)
      throws DatabaseException {

    super(
        database, keyClass, keyBinding, new EntityValueAdapter(entityClass, entityBinding, false));

    this.entityClass = entityClass;
    this.entityBinding = entityBinding;

    if (entityBinding instanceof PersistEntityBinding) {
      keyAssigner = ((PersistEntityBinding) entityBinding).getKeyAssigner();
    }
  }

  /**
   * Returns the primary key class for this index.
   *
   * @return the key class.
   */
  public Class<PK> getKeyClass() {
    return keyClass;
  }

  /**
   * Returns the primary key binding for this index.
   *
   * @return the key binding.
   */
  public EntryBinding<PK> getKeyBinding() {
    return keyBinding;
  }

  /**
   * Returns the entity class for this index.
   *
   * @return the entity class.
   */
  public Class<E> getEntityClass() {
    return entityClass;
  }

  /**
   * Returns the entity binding for this index.
   *
   * @return the entity binding.
   */
  public EntityBinding<E> getEntityBinding() {
    return entityBinding;
  }

  /**
   * Inserts an entity and returns null, or updates it if the primary key already exists and returns
   * the existing entity.
   *
   * <p>If a {@link PrimaryKey#sequence} is used and the primary key field of the given entity is
   * null or zero, this method will assign the next value from the sequence to the primary key field
   * of the given entity.
   *
   * <p>Auto-commit is used implicitly if the store is transactional.
   *
   * @param entity the entity to be inserted or updated.
   * @return the existing entity that was updated, or null if the entity was inserted.
   *     <!-- begin JE only -->
   * @throws OperationFailureException if one of the <a
   *     href="../je/OperationFailureException.html#writeFailures">Write Operation Failures</a>
   *     occurs.
   * @throws EnvironmentFailureException if an unexpected, internal or environment-wide failure
   *     occurs.
   *     <!-- end JE only -->
   * @throws DatabaseException the base class for all BDB exceptions.
   */
  public E put(E entity) throws DatabaseException {

    return put(null, entity);
  }

  /**
   * Inserts an entity and returns null, or updates it if the primary key already exists and returns
   * the existing entity.
   *
   * <p>If a {@link PrimaryKey#sequence} is used and the primary key field of the given entity is
   * null or zero, this method will assign the next value from the sequence to the primary key field
   * of the given entity.
   *
   * @param txn the transaction used to protect this operation, null to use auto-commit, or null if
   *     the store is non-transactional.
   * @param entity the entity to be inserted or updated.
   * @return the existing entity that was updated, or null if the entity was inserted.
   *     <!-- begin JE only -->
   * @throws OperationFailureException if one of the <a
   *     href="../je/OperationFailureException.html#writeFailures">Write Operation Failures</a>
   *     occurs.
   * @throws EnvironmentFailureException if an unexpected, internal or environment-wide failure
   *     occurs.
   *     <!-- end JE only -->
   * @throws DatabaseException the base class for all BDB exceptions.
   */
  public E put(Transaction txn, E entity) throws DatabaseException {

    DatabaseEntry keyEntry = new DatabaseEntry();
    DatabaseEntry dataEntry = new DatabaseEntry();
    assignKey(entity, keyEntry);

    boolean autoCommit = false;
    Environment env = db.getEnvironment();
    if (transactional && txn == null && DbCompat.getThreadTransaction(env) == null) {
      txn = env.beginTransaction(null, getAutoCommitTransactionConfig());
      autoCommit = true;
    }

    CursorConfig cursorConfig = null;
    if (concurrentDB) {
      cursorConfig = new CursorConfig();
      DbCompat.setWriteCursor(cursorConfig, true);
    }
    boolean failed = true;
    Cursor cursor = db.openCursor(txn, cursorConfig);
    LockMode lockMode = locking ? LockMode.RMW : null;
    try {
      while (true) {
        OperationStatus status = cursor.getSearchKey(keyEntry, dataEntry, lockMode);
        if (status == OperationStatus.SUCCESS) {
          E existing = entityBinding.entryToObject(keyEntry, dataEntry);
          entityBinding.objectToData(entity, dataEntry);
          cursor.put(keyEntry, dataEntry);
          failed = false;
          return existing;
        } else {
          entityBinding.objectToData(entity, dataEntry);
          status = cursor.putNoOverwrite(keyEntry, dataEntry);
          if (status != OperationStatus.KEYEXIST) {
            failed = false;
            return null;
          }
        }
      }
    } finally {
      cursor.close();
      if (autoCommit) {
        if (failed) {
          txn.abort();
        } else {
          txn.commit();
        }
      }
    }
  }

  /**
   * Inserts an entity, or updates it if the primary key already exists (does not return the
   * existing entity). This method may be used instead of {@link #put(Object)} to save the overhead
   * of returning the existing entity.
   *
   * <p>If a {@link PrimaryKey#sequence} is used and the primary key field of the given entity is
   * null or zero, this method will assign the next value from the sequence to the primary key field
   * of the given entity.
   *
   * <p>Auto-commit is used implicitly if the store is transactional.
   *
   * @param entity the entity to be inserted or updated.
   *     <!-- begin JE only -->
   * @throws OperationFailureException if one of the <a
   *     href="../je/OperationFailureException.html#writeFailures">Write Operation Failures</a>
   *     occurs.
   * @throws EnvironmentFailureException if an unexpected, internal or environment-wide failure
   *     occurs.
   *     <!-- end JE only -->
   * @throws DatabaseException the base class for all BDB exceptions.
   */
  public void putNoReturn(E entity) throws DatabaseException {

    putNoReturn(null, entity);
  }

  /**
   * Inserts an entity, or updates it if the primary key already exists (does not return the
   * existing entity). This method may be used instead of {@link #put(Transaction,Object)} to save
   * the overhead of returning the existing entity.
   *
   * <p>If a {@link PrimaryKey#sequence} is used and the primary key field of the given entity is
   * null or zero, this method will assign the next value from the sequence to the primary key field
   * of the given entity.
   *
   * @param txn the transaction used to protect this operation, null to use auto-commit, or null if
   *     the store is non-transactional.
   * @param entity the entity to be inserted or updated.
   *     <!-- begin JE only -->
   * @throws OperationFailureException if one of the <a
   *     href="../je/OperationFailureException.html#writeFailures">Write Operation Failures</a>
   *     occurs.
   * @throws EnvironmentFailureException if an unexpected, internal or environment-wide failure
   *     occurs.
   *     <!-- end JE only -->
   * @throws DatabaseException the base class for all BDB exceptions.
   */
  public void putNoReturn(Transaction txn, E entity) throws DatabaseException {

    /* <!-- begin JE only --> */
    if (DbCompat.IS_JE) {
      put(txn, entity, Put.OVERWRITE, null);
      return;
    }
    /* <!-- end JE only --> */

    DatabaseEntry keyEntry = new DatabaseEntry();
    DatabaseEntry dataEntry = new DatabaseEntry();
    assignKey(entity, keyEntry);
    entityBinding.objectToData(entity, dataEntry);

    db.put(txn, keyEntry, dataEntry);
  }

  /**
   * Inserts an entity and returns true, or returns false if the primary key already exists.
   *
   * <p>If a {@link PrimaryKey#sequence} is used and the primary key field of the given entity is
   * null or zero, this method will assign the next value from the sequence to the primary key field
   * of the given entity.
   *
   * <p>Auto-commit is used implicitly if the store is transactional.
   *
   * @param entity the entity to be inserted.
   * @return true if the entity was inserted, or false if an entity with the same primary key is
   *     already present.
   *     <!-- begin JE only -->
   * @throws OperationFailureException if one of the <a
   *     href="../je/OperationFailureException.html#writeFailures">Write Operation Failures</a>
   *     occurs.
   * @throws EnvironmentFailureException if an unexpected, internal or environment-wide failure
   *     occurs.
   *     <!-- end JE only -->
   * @throws DatabaseException the base class for all BDB exceptions.
   */
  public boolean putNoOverwrite(E entity) throws DatabaseException {

    return putNoOverwrite(null, entity);
  }

  /**
   * Inserts an entity and returns true, or returns false if the primary key already exists.
   *
   * <p>If a {@link PrimaryKey#sequence} is used and the primary key field of the given entity is
   * null or zero, this method will assign the next value from the sequence to the primary key field
   * of the given entity.
   *
   * @param txn the transaction used to protect this operation, null to use auto-commit, or null if
   *     the store is non-transactional.
   * @param entity the entity to be inserted.
   * @return true if the entity was inserted, or false if an entity with the same primary key is
   *     already present.
   *     <!-- begin JE only -->
   * @throws OperationFailureException if one of the <a
   *     href="../je/OperationFailureException.html#writeFailures">Write Operation Failures</a>
   *     occurs.
   * @throws EnvironmentFailureException if an unexpected, internal or environment-wide failure
   *     occurs.
   *     <!-- end JE only -->
   * @throws DatabaseException the base class for all BDB exceptions.
   */
  public boolean putNoOverwrite(Transaction txn, E entity) throws DatabaseException {

    /* <!-- begin JE only --> */
    if (DbCompat.IS_JE) {
      return put(txn, entity, Put.NO_OVERWRITE, null) != null;
    }
    /* <!-- end JE only --> */

    DatabaseEntry keyEntry = new DatabaseEntry();
    DatabaseEntry dataEntry = new DatabaseEntry();
    assignKey(entity, keyEntry);
    entityBinding.objectToData(entity, dataEntry);

    OperationStatus status = db.putNoOverwrite(txn, keyEntry, dataEntry);

    return (status == OperationStatus.SUCCESS);
  }

  /* <!-- begin JE only --> */
  /**
   * Inserts or updates an entity, using Put type and WriteOptions parameters, and returning an
   * OperationResult.
   *
   * <p>If a {@link PrimaryKey#sequence} is used and the primary key field of the given entity is
   * null or zero, this method will assign the next value from the sequence to the primary key field
   * of the given entity.
   *
   * @param txn the transaction used to protect this operation, null to use auto-commit, or null if
   *     the store is non-transactional.
   * @param entity the entity to be inserted.
   * @param putType is {@link Put#OVERWRITE} or {@link Put#NO_OVERWRITE}.
   * @param options the WriteOptions, or null to use default options.
   * @return the OperationResult if the record is written, else null. If {@code Put.NO_OVERWRITE} is
   *     used, null is returned if an entity with the same primary key is already present. If {@code
   *     Put.OVERWRITE} is used, null is never returned.
   * @throws OperationFailureException if one of the <a
   *     href="../je/OperationFailureException.html#writeFailures">Write Operation Failures</a>
   *     occurs.
   * @throws EnvironmentFailureException if an unexpected, internal or environment-wide failure
   *     occurs.
   * @throws DatabaseException the base class for all BDB exceptions.
   * @since 7.0
   */
  public OperationResult put(Transaction txn, E entity, Put putType, WriteOptions options) {

    if (putType != Put.OVERWRITE && putType != Put.NO_OVERWRITE) {
      throw new IllegalArgumentException("putType not allowed: " + putType);
    }

    DatabaseEntry keyEntry = new DatabaseEntry();
    DatabaseEntry dataEntry = new DatabaseEntry();
    assignKey(entity, keyEntry);
    entityBinding.objectToData(entity, dataEntry);

    return db.put(txn, keyEntry, dataEntry, putType, options);
  }
  /* <!-- end JE only --> */

  /**
   * If we are assigning primary keys from a sequence, assign the next key and set the primary key
   * field.
   */
  private void assignKey(E entity, DatabaseEntry keyEntry) throws DatabaseException {

    if (keyAssigner != null) {
      if (!keyAssigner.assignPrimaryKey(entity, keyEntry)) {
        entityBinding.objectToKey(entity, keyEntry);
      }
    } else {
      entityBinding.objectToKey(entity, keyEntry);
    }
  }

  /*
   * Of the EntityIndex methods only get()/map()/sortedMap() are implemented
   * here.  All other methods are implemented by BasicIndex.
   */

  public E get(PK key) throws DatabaseException {

    return get(null, key, null);
  }

  public E get(Transaction txn, PK key, LockMode lockMode) throws DatabaseException {

    /* <!-- begin JE only --> */
    if (DbCompat.IS_JE) {
      EntityResult<E> result = get(txn, key, Get.SEARCH, DbInternal.getReadOptions(lockMode));
      return result != null ? result.value() : null;
    }
    /* <!-- end JE only --> */

    DatabaseEntry keyEntry = new DatabaseEntry();
    DatabaseEntry dataEntry = new DatabaseEntry();
    keyBinding.objectToEntry(key, keyEntry);

    OperationStatus status = db.get(txn, keyEntry, dataEntry, lockMode);

    if (status == OperationStatus.SUCCESS) {
      return makeEntity(key, keyEntry, dataEntry);
    } else {
      return null;
    }
  }

  /* <!-- begin JE only --> */
  public EntityResult<E> get(Transaction txn, PK key, Get getType, ReadOptions options)
      throws DatabaseException {

    checkGetType(getType);

    DatabaseEntry keyEntry = new DatabaseEntry();
    DatabaseEntry dataEntry = new DatabaseEntry();
    keyBinding.objectToEntry(key, keyEntry);

    OperationResult result = db.get(txn, keyEntry, dataEntry, getType, options);

    if (result != null) {
      return new EntityResult<>(makeEntity(key, keyEntry, dataEntry), result);
    } else {
      return null;
    }
  }
  /* <!-- end JE only --> */

  private E makeEntity(PK key, DatabaseEntry keyEntry, DatabaseEntry dataEntry) {

    return (entityBinding instanceof PersistEntityBinding)
        ? (E) ((PersistEntityBinding) entityBinding).entryToObjectWithPriKey(key, dataEntry)
        : entityBinding.entryToObject(keyEntry, dataEntry);
  }

  public Map<PK, E> map() {
    return sortedMap();
  }

  public synchronized SortedMap<PK, E> sortedMap() {
    if (map == null) {
      map = new StoredSortedMap(db, keyBinding, entityBinding, true);
    }
    return map;
  }

  /**
   *
   * <!-- begin JE only -->
   *
   * @hidden
   *     <!-- end JE only -->
   *     For internal use only.
   *     <p>Used for obtaining the auto-commit txn config from the store, which overrides this
   *     method to return it.
   */
  /* <!-- begin JE only --> */
  protected
  /* <!-- end JE only --> */
  TransactionConfig getAutoCommitTransactionConfig() {
    return null;
  }

  boolean isUpdateAllowed() {
    return true;
  }
}
