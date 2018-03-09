/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DirNode extends Node implements Serializable, Iterable<Entry> {
    private static final long serialVersionUID = 8178778592344231767L;
    private final Set<Entry> entries = new HashSet<>();

    @Override
    public Iterator<Entry> iterator() {
        return entries.iterator();
    }

    public boolean addEntry(Entry entry) {
        boolean e = entries.add(entry);
        return e;
    }

    public boolean removeEntry(Entry entry) {
        boolean e = entries.remove(entry);
        return e;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DirNode entries1 = (DirNode) o;

        return entries.equals(entries1.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    public Node findByName(String name) {
        for (Entry e: entries
             ) {
            if(e.hashCode() == name.hashCode())
                return e.getNode();

        }
        return null;
    }
}
