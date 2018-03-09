/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import sdfs.exception.SDFSFileAlreadyExistException;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.NameNode;
import sdfs.namenode.SDFSFileChannel;
import sdfs.protocol.INameNodeProtocol;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NameNodeStub implements INameNodeProtocol {

    private InetSocketAddress nameAddress;

    public NameNodeStub(InetSocketAddress nameAddress) {
        this.nameAddress = nameAddress;
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        Object o = RPCInvoke("openReadonly", new Class<?>[]{fileUri.getClass()}, new Object[]{fileUri});
        if(o != null && o instanceof SDFSFileChannel) return (SDFSFileChannel) o;
        if(o != null && o instanceof FileNotFoundException) throw (FileNotFoundException) o;
        return null;
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException, IOException {
        Object o = RPCInvoke("openReadwrite", new Class<?>[]{fileUri.getClass()}, new Object[]{fileUri});
        if(o != null && o instanceof SDFSFileChannel) return (SDFSFileChannel) o;
        if(o != null && o instanceof FileNotFoundException) throw (FileNotFoundException) o;
        if(o != null && o instanceof IllegalStateException) throw new OverlappingFileLockException();
        return null;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IllegalStateException, IOException {
        Object o = RPCInvoke("create", new Class<?>[]{fileUri.getClass()}, new Object[]{fileUri});
        if(o != null && o instanceof SDFSFileChannel) return (SDFSFileChannel) o;
        if(o != null && o instanceof FileAlreadyExistsException) throw new SDFSFileAlreadyExistException();
        if(o != null && o instanceof FileNotFoundException) throw new FileNotFoundException();
        return null;
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException {
        Object o = RPCInvoke("closeReadonlyFile", new Class<?>[]{fileUuid.getClass()}, new Object[]{fileUuid});
        if(o != null && o instanceof IllegalStateException) throw (IllegalStateException) o;
    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {
        Object o = RPCInvoke("closeReadwriteFile", new Class<?>[]{fileUuid.getClass(), int.class}, new Object[]{fileUuid, newFileSize});
        if(o != null && o instanceof IllegalStateException) throw (IllegalStateException) o;
        if(o != null && o instanceof IllegalArgumentException) throw (IllegalArgumentException) o;
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        Object o = RPCInvoke("mkdir", new Class<?>[]{fileUri.getClass()}, new Object[]{fileUri});
        if(o != null && o instanceof FileAlreadyExistsException) throw new SDFSFileAlreadyExistException();
    }

    @Override
    public LocatedBlock addBlock(UUID fileUuid) {
        Object o = null;
        try {
            o = RPCInvoke("addBlock", new Class<?>[]{fileUuid.getClass()}, new Object[]{fileUuid});
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(o != null && o instanceof LocatedBlock) return (LocatedBlock) o;
        if(o != null && o instanceof IllegalStateException) throw (IllegalStateException) o;
        return null;
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
        Object o = null;
        try {
            o = RPCInvoke("addBlocks", new Class<?>[]{fileUuid.getClass(), int.class}, new Object[]{fileUuid, blockAmount});
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(o != null && o instanceof List) return (List<LocatedBlock>) o;
        if(o != null && o instanceof IllegalStateException) throw (IllegalStateException) o;
        return null;
    }

    @Override
    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
        Object o = null;
        try {
            o = RPCInvoke("removeLastBlock", new Class<?>[]{fileUuid.getClass()}, new Object[]{fileUuid});
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(o != null && o instanceof IllegalStateException) throw (IllegalStateException) o;
    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        Object o = null;
        try {
            o = RPCInvoke("removeLastBlocks", new Class<?>[]{fileUuid.getClass(), int.class}, new Object[]{fileUuid, blockAmount});
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(o != null && o instanceof IllegalStateException) throw (IllegalStateException) o;
    }

    public Object RPCInvoke(String methodName, Class<?>[] paraTypes, Object[] parameters) throws IOException {
        ObjectInputStream ois = null;
        ObjectOutputStream oos = null;

        Socket socket = new Socket(nameAddress.getHostName(), nameAddress.getPort());
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());

        oos.writeObject(methodName);
        oos.writeObject(paraTypes);
        oos.writeObject(parameters);
        oos.flush();


        Object o = null;

        try {
            o = ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }

        oos.close();
        ois.close();
        socket.close();
        return o;
    }
}
