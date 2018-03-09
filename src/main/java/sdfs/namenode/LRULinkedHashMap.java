package sdfs.namenode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by devilpi on 25/11/2017.
 */
public class LRULinkedHashMap<K, V> extends LinkedHashMap<K, V> implements Map<K, V> {

    private static int MAX_CAPACITY = 1024;

    private int capacity;

    public LRULinkedHashMap() {
        super();
        this.capacity = MAX_CAPACITY;
    }

    public LRULinkedHashMap(int initialCapacity, float loadFactor, boolean isLRU) {
        super(initialCapacity, loadFactor, isLRU);
        this.capacity = MAX_CAPACITY;
    }

    public LRULinkedHashMap(int initialCapacity, float loadFactor, boolean isLRU, int capacity) {
        super(initialCapacity, loadFactor, isLRU);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > capacity;
    }
}
