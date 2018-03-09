/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.datanode.DataNode;
import sdfs.namenode.NameNode;
import sdfs.protocol.IDataNodeProtocol;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.UUID;
import java.util.ArrayList;

public class DataNodeStub implements IDataNodeProtocol {

    private InetAddress dataAddress;

    public DataNodeStub(InetAddress dataAddress) {
        this.dataAddress = dataAddress;
    }

    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IndexOutOfBoundsException, IOException {
        Object o = RPCInvoke("read", new Class<?>[]{fileUuid.getClass(), int.class, int.class, int.class},
                new Object[]{fileUuid, blockNumber, offset, size});
        if(o != null && o instanceof byte[]) return (byte[]) o;
        if(o != null && o instanceof IndexOutOfBoundsException) throw (IndexOutOfBoundsException) o;
        return null;
    }

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IndexOutOfBoundsException, IOException {
        Object o = RPCInvoke("write", new Class<?>[]{fileUuid.getClass(), int.class, int.class, b.getClass()},
                new Object[]{fileUuid, blockNumber, offset, b});
        if(o != null && o instanceof IndexOutOfBoundsException) throw (IndexOutOfBoundsException) o;
    }

    public Object RPCInvoke(String methodName, Class<?>[] paraTypes, Object[] parameters) throws IOException {
        ObjectInputStream ois = null;
        ObjectOutputStream oos = null;

        Socket socket = new Socket(dataAddress.getHostName(), DataNode.DATA_NODE_PORT);

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
