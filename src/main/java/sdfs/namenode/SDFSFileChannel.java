/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import sdfs.client.DataNodeStub;
import sdfs.client.NameNodeStub;
import sdfs.datanode.DataNode;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.Entry;
import sdfs.filetree.FileNode;

import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.*;
import javafx.util.*;

public class SDFSFileChannel implements SeekableByteChannel, Flushable, Serializable {
    private static final long serialVersionUID = 6892411224902751501L;
    private final UUID uuid; //File uuid
    private int fileSize; //Size of this file
    private int blockAmount; //Total block amount of this file
    private final FileNode fileNode;
    private final boolean isReadOnly;

    private int cacheSize;
    private NameNodeStub nameNodeStub;
    private Map<LocatedBlock, Pair<Boolean, byte[]> > dataBlocksCache; //BlockNumber to DataBlock cache. byte[] or ByteBuffer are both acceptable.

    private long pos;
    private boolean closed;

    SDFSFileChannel(UUID uuid, int fileSize, int blockAmount, FileNode fileNode, boolean isReadOnly) {
        this.uuid = uuid;
        this.fileSize = fileSize;
        this.blockAmount = blockAmount;
        this.fileNode = fileNode;
        this.isReadOnly = isReadOnly;

        this.pos = 0;
        this.closed = false;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
        dataBlocksCache = new LRULinkedHashMap<>(0, 0.75f, true, cacheSize);
    }

    public void setNameNode(NameNodeStub nameNodeStub) {
        this.nameNodeStub = nameNodeStub;
    }

    public void setAppend(boolean append) {
        if(append) pos = fileSize;
    }

    private void putOffHead() {
        java.util.Map.Entry<LocatedBlock, Pair<Boolean, byte[]>> entry = dataBlocksCache.entrySet().iterator().next();
        if(entry.getValue().getKey()) delayWriting(entry.getKey(), entry.getValue().getValue());
    }

    private void delayWriting(LocatedBlock locatedBlock, byte[] b) {
        DataNodeStub dataNodeStub = new DataNodeStub(locatedBlock.getInetAddress());
        try {
            dataNodeStub.write(uuid, locatedBlock.getBlockNumber(), 0, b);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void readCache(ByteBuffer dst, LocatedBlock locatedBlock, int offset, int size) {
        if(!dataBlocksCache.containsKey(locatedBlock)) {
            if(dataBlocksCache.size() == cacheSize) putOffHead();
            DataNodeStub dataNodeStub = new DataNodeStub(locatedBlock.getInetAddress());
            try {
                byte[] b = dataNodeStub.read(uuid, locatedBlock.getBlockNumber(), 0, DataNode.BLOCK_SIZE);
                dataBlocksCache.put(locatedBlock, new Pair<>(false, b));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        byte[] b = dataBlocksCache.get(locatedBlock).getValue();
        if(offset >= b.length) return;
        dst.put(b, offset, Integer.min(size, b.length - offset));
    }

    private void writeCache(ByteBuffer src, LocatedBlock locatedBlock, int offset, int size) {
        if(!dataBlocksCache.containsKey(locatedBlock)) {
            if(dataBlocksCache.size() == cacheSize) putOffHead();
            byte[] b = new byte[size];
            dataBlocksCache.put(locatedBlock, new Pair<>(true, b));
        }
        Pair<Boolean, byte[]> pair = dataBlocksCache.get(locatedBlock);
        byte[] bytes = new byte[size + offset];
        System.arraycopy(pair.getValue(), 0, bytes, 0, Integer.min(offset, pair.getValue().length));
        src.get(bytes, offset, size);
        dataBlocksCache.put(locatedBlock, new Pair<>(true, bytes));
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if(closed) throw new ClosedChannelException();
        if(pos > fileSize) return 0;
        int len = dst.remaining();
        int startBlock = (int) (pos / DataNode.BLOCK_SIZE);
        int endBlock = Integer.min((int)((pos + len - 1) / DataNode.BLOCK_SIZE), blockAmount - 1);

        for(int index = startBlock; index <= endBlock; index ++) {
            int startByte = Integer.max(index * DataNode.BLOCK_SIZE, (int) pos);
            int endByte = Integer.min((index + 1) * DataNode.BLOCK_SIZE - 1, (int) pos + len - 1);
            int size = endByte - startByte + 1;

            LocatedBlock locatedBlock = fileNode.getBlock(index);
            if(locatedBlock == null) break;

            readCache(dst, locatedBlock, startByte - index * DataNode.BLOCK_SIZE, size);
        }

        int byteRead = len - dst.remaining();
        pos += byteRead;
        return byteRead;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if(closed) throw new ClosedChannelException();
        if(isReadOnly) throw new NonWritableChannelException();
        int len = src.remaining();
        if(len == 0) return 0;
        int startBlock = (int) (pos / DataNode.BLOCK_SIZE);
        int endBlock = (int) ((pos + len - 1) / DataNode.BLOCK_SIZE);

        if(endBlock >= blockAmount) {
            List<LocatedBlock> list = nameNodeStub.addBlocks(uuid, endBlock + 1 - blockAmount);
            for (LocatedBlock l: list
                 ) {
                BlockInfo b = new BlockInfo();
                b.addLocatedBlock(l);
                fileNode.addBlockInfo(b);
            }
            if(pos > fileSize) {
                for(int i = blockAmount - 1; i <= startBlock; i ++) {
                    int startByte = Integer.max(i * DataNode.BLOCK_SIZE, fileSize);
                    int endByte = Integer.min((i + 1) * DataNode.BLOCK_SIZE - 1, (int) pos - 1);
                    int size = endByte - startByte + 1;
                    fillZero(fileNode.getBlock(i), startByte - i * DataNode.BLOCK_SIZE, size);
                }
            }
            blockAmount += list.size();
        }

        for(int index = startBlock; index <= endBlock; index ++) {
            int startByte = Integer.max(index * DataNode.BLOCK_SIZE, (int) pos);
            int endByte = Integer.min((index + 1) * DataNode.BLOCK_SIZE - 1, (int) pos + len - 1);
            int size = endByte - startByte + 1;

            LocatedBlock locatedBlock = fileNode.getBlock(index);
            if(locatedBlock == null) break;

            writeCache(src, locatedBlock, startByte - index * DataNode.BLOCK_SIZE, size);
        }

        int byteWrite = len - src.remaining();
        pos += byteWrite;
        fileSize = (int) pos;
        return (byteWrite == 0) ? -1 : byteWrite;
    }

    public void fillZero(LocatedBlock locatedBlock, int offset, int size) {
        byte b[] = new byte[size];
        ByteBuffer byteBuffer = ByteBuffer.wrap(b);
        writeCache(byteBuffer, locatedBlock, offset, size);
    }

    @Override
    public long position() throws IOException {
        if(closed) throw new ClosedChannelException();
        return pos;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if(closed) throw new ClosedChannelException();
        if(newPosition < 0) throw new IllegalArgumentException();

        pos = newPosition;
        return this;
    }

    @Override
    public long size() throws IOException {
        if(closed) throw new ClosedChannelException();
        return fileSize;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        if(closed) throw new ClosedChannelException();
        if(size < 0) throw new IllegalArgumentException();
        if(isReadOnly) throw new NonWritableChannelException();

        if(size < fileSize) {
            long newBlockNum = size / DataNode.BLOCK_SIZE + (size % DataNode.BLOCK_SIZE == 0 ? 0 : 1);
            long curBlockNum = fileSize / DataNode.BLOCK_SIZE + (fileSize % DataNode.BLOCK_SIZE == 0 ? 0 : 1);

            int lastBlockSize = (int)size - (int)(newBlockNum - 1) * DataNode.BLOCK_SIZE;
            ByteBuffer byteBuffer = ByteBuffer.allocate(lastBlockSize);
            readCache(byteBuffer, fileNode.getBlock((int)newBlockNum), 0, lastBlockSize);
            byteBuffer.flip();
            writeCache(byteBuffer, fileNode.getBlock((int)newBlockNum), 0, lastBlockSize);
            byteBuffer.clear();

            nameNodeStub.removeLastBlocks(uuid, (int)(curBlockNum - newBlockNum));
            blockAmount -= (int)(curBlockNum - newBlockNum);
            for(int i = 0; i < (int)(curBlockNum - newBlockNum); i ++) fileNode.removeLastBlockInfo();
            fileSize = (int)size;
        }

        if(pos > size) pos = size;
        return this;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws IOException {
        if(closed) return;
        flush();
        closed = true;
        if(isReadOnly) nameNodeStub.closeReadonlyFile(uuid);
        else {
            int curBlockNum = fileSize / DataNode.BLOCK_SIZE + (fileSize % DataNode.BLOCK_SIZE == 0 ? 0 : 1);
            if(curBlockNum < blockAmount) {
                nameNodeStub.removeLastBlocks(uuid, blockAmount - curBlockNum);
                blockAmount = curBlockNum;
            }
            nameNodeStub.closeReadwriteFile(uuid, fileSize);
        }
    }

    @Override
    public void flush() throws IOException {
        if(closed) throw new ClosedChannelException();
        int delayCount = 0;
        for (java.util.Map.Entry<LocatedBlock, Pair<Boolean, byte[]>> entry: dataBlocksCache.entrySet()
             ) {
            if(entry.getValue().getKey()) {
                delayCount ++;
                delayWriting(entry.getKey(), entry.getValue().getValue());
            }
        }

        while(delayCount > 0) {
            LocatedBlock l = null;
            Pair<Boolean, byte[]> pair = null;
            for (java.util.Map.Entry<LocatedBlock, Pair<Boolean, byte[]>> entry: dataBlocksCache.entrySet()
                    ) {
                if(entry.getValue().getKey()) {
                    l = entry.getKey();
                    pair = entry.getValue();
                    break;
                }
            }
            if(pair == null) break;
            dataBlocksCache.put(l, new Pair<>(false, pair.getValue()));
            delayCount --;
        }
    }
}
