/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import java.io.Serializable;

public class Entry implements Serializable {
    private static final long serialVersionUID = 8178176732984379L;
    private final Node node;
    private String name;

    public Entry(String name, Node node) {
        if (name == null || node == null) {
            throw new NullPointerException();
        }
        if (name.isEmpty() || name.contains("/")) {
            throw new IllegalArgumentException();
        }
        this.name = name;
        this.node = node;
    }

    public void setName(String name) {
        if (name == null) {
            throw new NullPointerException();
        }
        if (name.isEmpty() || name.contains("/")) {
            throw new IllegalArgumentException();
        }
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Entry entry = (Entry) o;

        return name.equals(entry.name);

    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public Node getNode() {
        return node;
    }
}
