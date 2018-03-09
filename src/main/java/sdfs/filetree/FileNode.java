/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import sdfs.namenode.LocatedBlock;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileNode extends Node implements Serializable, Iterable<BlockInfo> {
    private static final long serialVersionUID = -5007570814999866661L;
    private final List<BlockInfo> blockInfos = new ArrayList<>();
    private int fileSize;//file size should be checked when closing the file.

    public FileNode() {
        this.fileSize = 0;
    }

    public void addBlockInfo(BlockInfo blockInfo) {
        blockInfos.add(blockInfo);
    }

    public BlockInfo removeLastBlockInfo() {
        return blockInfos.remove(blockInfos.size() - 1);
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public int getBlockAmount() {
        return blockInfos.size();
    }

    public LocatedBlock getBlock(int index) {
        if(blockInfos.size() <= index) return null;
        for (LocatedBlock l: blockInfos.get(index)
             ) {
            if(l != null) return l;
        }
        return null;
    }

    public BlockInfo getBlockInfo(int index) {
        return blockInfos.get(index);
    }

    @Override
    public Iterator<BlockInfo> iterator() {
        return blockInfos.listIterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileNode that = (FileNode) o;

        return blockInfos.equals(that.blockInfos);
    }

    @Override
    public int hashCode() {
        return blockInfos.hashCode();
    }


    public FileNode deepClone() {
        FileNode fileNode = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(this);
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bis);
            fileNode = (FileNode) ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fileNode;
    }

    public void getValue(FileNode fileNode) {
        this.fileSize = fileNode.getFileSize();
        blockInfos.clear();
        for(int i = 0; i < fileNode.getBlockAmount(); i ++) {
            blockInfos.add(fileNode.getBlockInfo(i));
        }
    }

}

