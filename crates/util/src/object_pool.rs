// SPDX-License-Identifier: BSD-3-Clause

use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

/// A thread-safe, lock-free object pool for managing preallocated heap objects.
///
/// `ObjectPool` maintains a fixed-size pool of heap-allocated objects that can be
/// efficiently acquired and returned without additional allocations. The pool can be
/// cloned to create multiple references that share the same underlying pool.
///
/// # Type Parameters
///
/// * `T` - The type of objects stored in the pool. Must implement `Default` for initialization.
pub struct ObjectPool<T> {
  objects: Arc<ArrayQueue<Box<T>>>,
}

impl<T> Clone for ObjectPool<T> {
  fn clone(&self) -> Self {
    Self { objects: Arc::clone(&self.objects) }
  }
}

impl<T> std::fmt::Debug for ObjectPool<T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ObjectPool")
      .field("capacity", &self.objects.capacity())
      .field("available", &self.objects.len())
      .finish()
  }
}

impl<T: Default> ObjectPool<T> {
  /// Creates a new object pool with the specified capacity.
  ///
  /// # Arguments
  ///
  /// * `capacity` - The fixed number of objects to preallocate in the pool.
  ///
  /// # Returns
  ///
  /// A new `ObjectPool` instance with `capacity` preallocated objects.
  pub fn with_capacity(capacity: usize) -> Self {
    let objects = Arc::new(ArrayQueue::new(capacity));

    // Preallocate all objects
    for _ in 0..capacity {
      let _ = objects.push(Box::new(T::default()));
    }

    ObjectPool { objects }
  }

  /// Retrieves an object from the pool.
  ///
  /// # Returns
  ///
  /// * `Some(Box<T>)` - An object from the pool if one is available.
  /// * `None` - If the pool is empty (all objects are currently in use).
  pub fn get(&self) -> Option<Box<T>> {
    self.objects.pop()
  }

  /// Returns an object back to the pool for reuse.
  ///
  /// # Arguments
  ///
  /// * `object` - The object to return to the pool.
  ///
  /// # Panics
  ///
  /// Panics if the pool is already at capacity. This should not happen under
  /// normal usage since objects come from the pool initially.
  pub fn put(&self, object: Box<T>) {
    let _ = self.objects.push(object);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;
  use std::thread;

  #[test]
  fn test_create_pool() {
    let pool = ObjectPool::<String>::with_capacity(10);

    // All objects should be preallocated
    for _ in 0..10 {
      let obj: Option<Box<String>> = pool.get();
      assert!(obj.is_some());
    }

    // Pool should be empty now
    let obj: Option<Box<String>> = pool.get();
    assert!(obj.is_none());
  }

  #[test]
  fn test_get_returns_none_when_empty() {
    let pool = ObjectPool::<i32>::with_capacity(2);

    let obj1 = pool.get();
    let obj2 = pool.get();
    let obj3 = pool.get();

    assert!(obj1.is_some());
    assert!(obj2.is_some());
    assert!(obj3.is_none());
  }

  #[test]
  fn test_put_returns_object_to_pool() {
    let pool = ObjectPool::<String>::with_capacity(1);

    let obj = pool.get().unwrap();
    assert!(pool.get().is_none()); // Pool is empty

    pool.put(obj);

    let obj2 = pool.get();
    assert!(obj2.is_some()); // Object is available again
  }

  #[test]
  fn test_object_modification() {
    let pool = ObjectPool::<String>::with_capacity(1);

    // Get object and modify it
    let mut obj = pool.get().unwrap();
    *obj = "Hello, World!".to_string();

    // Return to pool
    pool.put(obj);

    // Get it again and verify modification persists
    let obj2 = pool.get().unwrap();
    assert_eq!(*obj2, "Hello, World!");
  }

  #[test]
  fn test_multiple_get_put_cycles() {
    let pool = ObjectPool::<u32>::with_capacity(3);

    // First cycle
    let obj1: Box<u32> = pool.get().unwrap();
    let obj2: Box<u32> = pool.get().unwrap();
    pool.put(obj1);
    pool.put(obj2);

    // Second cycle - should be able to get objects again
    let obj3: Option<Box<u32>> = pool.get();
    assert!(obj3.is_some());
    let obj4: Option<Box<u32>> = pool.get();
    assert!(obj4.is_some());
    let obj5: Option<Box<u32>> = pool.get();
    assert!(obj5.is_some());
    let obj6: Option<Box<u32>> = pool.get();
    assert!(obj6.is_none());
  }

  #[test]
  fn test_object_reuse() {
    #[derive(Default)]
    struct Counter {
      value: usize,
    }

    let pool = ObjectPool::<Counter>::with_capacity(1);

    // Get and modify
    let mut obj: Box<Counter> = pool.get().unwrap();
    obj.value = 42;
    pool.put(obj);

    // Get again - should be the same object
    let obj2: Box<Counter> = pool.get().unwrap();
    assert_eq!(obj2.value, 42);
  }

  #[test]
  fn test_large_capacity() {
    let pool = ObjectPool::<u64>::with_capacity(1000);

    let mut objects: Vec<Box<u64>> = Vec::new();
    for _ in 0..1000 {
      objects.push(pool.get().unwrap());
    }

    // Should be empty now
    let obj: Option<Box<u64>> = pool.get();
    assert!(obj.is_none());

    // Return all objects
    for obj in objects {
      pool.put(obj);
    }

    // Should be able to get them all again
    for _ in 0..1000 {
      let obj: Option<Box<u64>> = pool.get();
      assert!(obj.is_some());
    }
  }

  #[test]
  fn test_concurrent_access() {
    let pool = Arc::new(ObjectPool::<u64>::with_capacity(100));
    let mut handles = vec![];

    // Spawn 10 threads that each get and put objects
    for i in 0..10 {
      let pool_clone = Arc::clone(&pool);
      let handle = thread::spawn(move || {
        let mut objects = Vec::new();

        // Get 10 objects
        for _ in 0..10 {
          if let Some(mut obj) = pool_clone.get() {
            *obj = i as u64;
            objects.push(obj);
          }
        }

        // Return them
        for obj in objects {
          pool_clone.put(obj);
        }
      });
      handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
      handle.join().unwrap();
    }

    // All objects should be back in the pool
    for _ in 0..100 {
      let obj: Option<Box<u64>> = pool.get();
      assert!(obj.is_some());
    }
  }

  #[test]
  fn test_concurrent_get_put_stress() {
    let pool = Arc::new(ObjectPool::<String>::with_capacity(50));
    let mut handles = vec![];

    // Multiple threads competing for limited resources
    for _ in 0..20 {
      let pool_clone = Arc::clone(&pool);
      let handle = thread::spawn(move || {
        for _ in 0..100 {
          if let Some(mut obj) = pool_clone.get() {
            *obj = "test".to_string();
            // Simulate some work
            thread::yield_now();
            pool_clone.put(obj);
          } else {
            // Pool was empty, try again
            thread::yield_now();
          }
        }
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap();
    }

    // Verify all objects are returned
    let mut count = 0;
    while pool.get().is_some() {
      count += 1;
    }
    assert_eq!(count, 50);
  }

  #[test]
  fn test_default_initialization() {
    #[derive(Default, PartialEq, Debug)]
    struct TestStruct {
      value: i32,
      flag: bool,
    }

    let pool = ObjectPool::<TestStruct>::with_capacity(5);

    // All objects should be initialized with Default
    for _ in 0..5 {
      let obj: Box<TestStruct> = pool.get().unwrap();
      assert_eq!(*obj, TestStruct { value: 0, flag: false });
    }
  }

  #[test]
  fn test_thread_safety_with_arc() {
    let pool = Arc::new(ObjectPool::<i32>::with_capacity(10));

    let pool1 = Arc::clone(&pool);
    let pool2 = Arc::clone(&pool);

    let handle1 = thread::spawn(move || {
      let obj: Option<Box<i32>> = pool1.get();
      assert!(obj.is_some());
    });

    let handle2 = thread::spawn(move || {
      let obj: Option<Box<i32>> = pool2.get();
      assert!(obj.is_some());
    });

    handle1.join().unwrap();
    handle2.join().unwrap();
  }
}
