package io.github.zhangjig.transfer.benchmark.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

public class ConcurrentReadMap<K,V> implements Map<K, V>, ConcurrentMap<K, V> {

	private volatile Map<K,V> target = Collections.emptyMap();
	
	@Override
	synchronized public V put(K key, V value) {
		V old = target.get(key);
		if(old == value || (value != null && value.equals(old))) {
			return old;
		}
		Map<K,V> map = new HashMap<>(target);
		old = map.put(key, value);
		this.target = map;
		return old;
	}

	@Override
	synchronized public V remove(Object key) {
		if(!target.containsKey(key)) {
			return null;
		}
		Map<K,V> map = new HashMap<>(target);
		V obj = map.remove(key);
		this.target = map;
		return obj;
	}

	@Override
	synchronized public void putAll(Map<? extends K, ? extends V> m) {
		Map<K,V> map = new HashMap<>(target);
		map.putAll(m);
		this.target = map;
	}

	@Override
	synchronized public void clear() {
		this.target = Collections.emptyMap();
	}
	
	@Override
	synchronized public V putIfAbsent(K key, V value) {
		V old = target.get(key);
		if(old != null) {
			return old;
		}
		Map<K,V> map = new HashMap<>(target);
		old = map.putIfAbsent(key, value);
		this.target = map;
		return old;
	}

	@Override
	synchronized public boolean remove(Object key, Object value) {
		if(!target.containsKey(key)) {
			return false;
		}
		Map<K,V> map = new HashMap<>(target);
		boolean modified = map.remove(key, value);
		this.target = map;
		return modified;
	}

	@Override
	synchronized public boolean replace(K key, V oldValue, V newValue) {
		Object old = target.get(key);
		if(!Objects.equals(old, oldValue)) {
			return false;
		}
		Map<K,V> map = new HashMap<>(target);
		boolean modified = map.replace(key, oldValue, newValue);
		this.target = map;
		return modified;
	}

	@Override
	synchronized public V replace(K key, V value) {
		Map<K,V> map = new HashMap<>(target);
		V obj = map.replace(key, value);
		this.target = map;
		return obj;
	}
	
	@Override
	public int size() {
		return target.size();
	}

	@Override
	public boolean isEmpty() {
		return target.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return target.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return target.containsKey(value);
	}

	@Override
	public V get(Object key) {
		return target.get(key);
	}

	@Override
	public Set<K> keySet() {
		return target.keySet();
	}

	@Override
	public Collection<V> values() {
		return target.values();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return target.entrySet();
	}
	
}
