/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.namenode.NameNode;
import sdfs.namenode.SDFSFileChannel;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SimpleDistributedFileSystem implements ISimpleDistributedFileSystem {
    private NameNodeStub nameNodeStub;
    private int cacheSize;
    /**
     * @param fileDataBlockCacheSize Buffer size for file data block. By default, it should be 16.
     *                               That means 16 block of data will be cache on local.
     *                               And you should use LRU algorithm to replace it.
     *                               It may be change during test. So don't assert it will equal to a constant.
     */
    public SimpleDistributedFileSystem(InetSocketAddress nameNodeAddress, int fileDataBlockCacheSize) {
        this.nameNodeStub = new NameNodeStub(nameNodeAddress);
        this.cacheSize = fileDataBlockCacheSize;
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        SDFSFileChannel sdfsFileChannel = nameNodeStub.openReadonly(fileUri);
        sdfsFileChannel.setNameNode(nameNodeStub);
        sdfsFileChannel.setCacheSize(cacheSize);
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        SDFSFileChannel sdfsFileChannel = nameNodeStub.create(fileUri);
        sdfsFileChannel.setNameNode(nameNodeStub);
        sdfsFileChannel.setCacheSize(cacheSize);
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel openReadWrite(String fileUri) throws IOException {
        SDFSFileChannel sdfsFileChannel = nameNodeStub.openReadwrite(fileUri);
        sdfsFileChannel.setNameNode(nameNodeStub);
        sdfsFileChannel.setCacheSize(cacheSize);
        return sdfsFileChannel;
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        nameNodeStub.mkdir(fileUri);
    }
}
