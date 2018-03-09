/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.datanode;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import sdfs.namenode.NameNode;
import sdfs.protocol.IDataNodeProtocol;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.file.FileAlreadyExistsException;
import java.util.UUID;
import java.util.*;
import java.net.*;

public class DataNode implements IDataNodeProtocol {
    /**
     * The block size may be changed during test.
     * So please use this constant.
     */
    public static final int BLOCK_SIZE = 128 * 1024;
    public static final int DATA_NODE_PORT = 4342;
    public static final String path = "block/";
    //    put off due to its difficulties
    //private final Map<UUID, Set<Integer>> uuidReadonlyPermissionCache = new HashMap<>();
    //private final Map<UUID, Set<Integer>> uuidReadwritePermissionCache = new HashMap<>();
    public static void main(String[] args) throws IOException {
        DataNode dataNode = new DataNode();
        dataNode.init();
    }

    public DataNode() {
        File pathFile = new File(path);
        if(!pathFile.exists()) {
            pathFile.mkdirs();
        }
    }

    public void init() throws IOException {
        ServerSocket serverSocket = new ServerSocket(DATA_NODE_PORT);
        send();
        while (true) {
            Socket socket = serverSocket.accept();
            invoke(socket, this);
        }
    }

    public void send() throws IOException {
        Socket socket = new Socket("localhost", NameNode.NAME_NODE_PORT);
        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

        oos.writeObject("newDataNode");
        String hostname = "localhost";
        Class<?>[] paraTypes = {hostname.getClass()};
        Object[] parameters = {hostname};
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
        if(o != null && o instanceof IOException) throw (IOException) o;
    }

    public void invoke(Socket socket, DataNode dataNode) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ObjectInputStream ois = null;
                ObjectOutputStream oos = null;

                try {
                    ois = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
                    oos = new ObjectOutputStream(socket.getOutputStream());
                    String operate = (String) ois.readObject();
                    Class<?>[] paraTypes = (Class<?>[]) ois.readObject();
                    Object[] parameters = (Object[]) ois.readObject();

                    Method method = dataNode.getClass().getDeclaredMethod(operate, paraTypes);
                    try {
                        Object ret = method.invoke(dataNode, parameters);
                        oos.writeObject(ret);
                    } catch (Exception e) {
                        oos.writeObject(e.getCause());
                    }
                    oos.flush();

                    ois.close();
                    oos.close();

                    socket.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IndexOutOfBoundsException, IOException {
        String filename = path + blockNumber + ".block";

        File blockFile = new File(filename);

        if(!blockFile.exists()) throw new FileNotFoundException();
        if(offset < 0 || offset + size > BLOCK_SIZE) throw new IndexOutOfBoundsException();

        RandomAccessFile randomAccessFile = new RandomAccessFile(filename, "r");
        randomAccessFile.seek(offset);
        byte[] b = new byte[size];
        int byteRead = randomAccessFile.read(b, 0, size);
        randomAccessFile.close();
        byte[] ret = new byte[byteRead];
        System.arraycopy(b, 0, ret, 0, byteRead);
        return ret;
    }

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IndexOutOfBoundsException, IOException {
        String filename = path + blockNumber + ".block";

        File blockFile = new File(filename);

        if(blockFile.exists()) throw new FileAlreadyExistsException("block exists");
        if(offset < 0 || offset + b.length > BLOCK_SIZE) throw new IndexOutOfBoundsException();

        RandomAccessFile randomAccessFile = new RandomAccessFile(filename, "rw");
        randomAccessFile.seek(offset);
        randomAccessFile.write(b, 0, b.length);
        randomAccessFile.close();
    }
}
