use core::fmt::Debug;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

pub trait LogError {
    #[must_use = "this `Result` may be an `Err` variant, which should be handled"]
    fn log(self) -> Self;
}

impl<T, E: Debug> LogError for Result<T, E> {
    #[inline]
    #[track_caller]
    fn log(self) -> Self {
        if let Err(err) = &self {
            let location = std::panic::Location::caller();
            eprintln!(
                "\x1b[31mcalled `Result::log()` on an `Err` at {}:{}:{} for value:\n{:?} \x1b[0m",
                location.file(),
                location.line(),
                location.column(),
                err,
            );
        }
        self
    }
}

pub trait HashMapWithVectorValue<K, V> {
    fn insert_append(&mut self, k: K, v: Vec<V>);
}

impl<K: Eq + Hash, V> HashMapWithVectorValue<K, V> for HashMap<K, Vec<V>> {
    fn insert_append(&mut self, k: K, mut v: Vec<V>) {
        self.entry(k).or_default().append(&mut v);
    }
}

pub trait HashMapWithHashMapValue<K1, K2, V> {
    fn nested_insert(&mut self, k1: K1, k2: K2, v: V);
}

impl<K1: Eq + Hash, K2: Eq + Hash, V> HashMapWithHashMapValue<K1, K2, V>
    for HashMap<K1, HashMap<K2, V>>
{
    fn nested_insert(&mut self, k1: K1, k2: K2, v: V) {
        self.entry(k1).or_default().insert(k2, v);
    }
}

pub trait ReadAndSet<T: Clone> {
    fn read(&self) -> T;
    fn put(&self, v: T);
}

impl<T: Clone> ReadAndSet<T> for Arc<RwLock<T>> {
    fn read(&self) -> T {
        RwLock::read(self).unwrap().clone()
    }

    fn put(&self, v: T) {
        *self.write().unwrap() = v;
    }
}

impl<T: Clone> ReadAndSet<T> for Mutex<T> {
    fn read(&self) -> T {
        Mutex::lock(self).unwrap().clone()
    }

    fn put(&self, v: T) {
        *self.lock().unwrap() = v;
    }
}

pub trait MakeOptionIfEmpty: Sized {
    fn none_if_empty(self) -> Option<Self>;
}

impl MakeOptionIfEmpty for String {
    fn none_if_empty(self) -> Option<Self> {
        if self.is_empty() {
            return None;
        }
        Some(self)
    }
}
