/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import sdfs.datanode.DataNode;
import sdfs.filetree.*;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.lang.reflect.*;

import java.net.*;

public class NameNode implements INameNodeProtocol, INameNodeDataNodeProtocol {
    public static final int NAME_NODE_PORT = 4341;
    private final Map<UUID, FileNode> readonlyFile = new HashMap<>();
    private final Map<UUID, FileNode> readwritePFile = new HashMap<>();
    private final Map<String, BitSet> dataNodes = new HashMap<>();
    private final Map<UUID, FileNode> readwriteCopy = new HashMap<>();

    private DirNode rootNode = new DirNode();
    private static final String path = "node/";

    public static void main(String[] args) throws IOException {
        NameNode nameNode = new NameNode();
        nameNode.onRPCReceive();
    }

    public NameNode() {
        File pathFile = new File(path);
        if(!pathFile.exists()) {
            pathFile.mkdirs();
        }

        String fileTree = path + "fileTree.node";
        try {
            File file = new File(fileTree);
            if(file.exists()) {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
                rootNode = (DirNode) ois.readObject();
                ois.close();
            } else {
                file.createNewFile();
                ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
                oos.writeObject(rootNode);
                oos.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            if(pathFile.isDirectory()) {
                File[] files = pathFile.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(".info");
                    }
                });
                for (File data: files
                        ) {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(data));
                    BitSet bitSet = (BitSet) ois.readObject();
                    ois.close();
                    dataNodes.put(data.getName().substring(0, data.getName().length() - 5), bitSet);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void onRPCReceive() throws IOException {
        ServerSocket serverSocket = new ServerSocket(NAME_NODE_PORT);

        while(true) {
            Socket socket = serverSocket.accept();
            invoke(socket, this);
        }
    }

    public void invoke(Socket socket, NameNode nameNode) throws IOException {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ObjectInputStream ois = null;
                ObjectOutputStream oos = null;

                try {
                    ois = new ObjectInputStream(socket.getInputStream());
                    oos = new ObjectOutputStream(socket.getOutputStream());
                    String operate = (String) ois.readObject();
                    Class<?>[] paraTypes = (Class<?>[]) ois.readObject();
                    Object[] parameters = (Object[]) ois.readObject();

                    Method method = nameNode.getClass().getDeclaredMethod(operate, paraTypes);
                    try {
                        Object ret = method.invoke(nameNode, parameters);
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
        }
        ).start();
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException, FileNotFoundException {
        FileNode node = (FileNode) parseUri(rootNode, fileUri);
        if(node == null) throw new FileNotFoundException();
        UUID uuid = UUID.randomUUID();
        readonlyFile.put(uuid, node.deepClone());
        return new SDFSFileChannel(uuid, node.getFileSize(), node.getBlockAmount(), node, true);
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException, IOException {
        FileNode node = (FileNode) parseUri(rootNode, fileUri);
        if(node == null) throw new FileNotFoundException();
        if(readwriteCopy.containsValue(node)) throw new IllegalStateException();
        UUID uuid = UUID.randomUUID();
        readwritePFile.put(uuid, node.deepClone());
        readwriteCopy.put(uuid, node);
        return new SDFSFileChannel(uuid, node.getFileSize(), node.getBlockAmount(), node, false);
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException, FileAlreadyExistsException {
        if(fileUri.charAt(0) != '/') throw new FileNotFoundException();
        DirNode dirNode = null;
        if(fileUri.indexOf('/', 1) == -1) {
            dirNode = rootNode;
        } else {
            dirNode = (DirNode) parseUri(rootNode, fileUri.substring(0, fileUri.lastIndexOf('/')));
        }
        String name = fileUri.substring(fileUri.lastIndexOf('/') + 1, fileUri.length());
        if(dirNode.findByName(name) != null) throw new FileAlreadyExistsException("file already exist");
        FileNode fileNode = new FileNode();
        dirNode.addEntry(new Entry(name, fileNode));

        UUID uuid = UUID.randomUUID();
        readwritePFile.put(uuid, fileNode.deepClone());
        readwriteCopy.put(uuid, fileNode);
        return new SDFSFileChannel(uuid, fileNode.getFileSize(), fileNode.getBlockAmount(), fileNode, false);
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException {
        if(!readonlyFile.containsKey(fileUuid)) throw new IllegalStateException();
        readonlyFile.remove(fileUuid);
    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {
        if(!readwritePFile.containsKey(fileUuid)) throw new IllegalStateException();
        FileNode fileNode = readwritePFile.get(fileUuid);
        if(fileNode.getBlockAmount() * DataNode.BLOCK_SIZE < newFileSize ||
                (fileNode.getBlockAmount() - 1) * DataNode.BLOCK_SIZE >= newFileSize)
            throw new IllegalArgumentException();
        readwritePFile.remove(fileUuid);
        fileNode.setFileSize(newFileSize);

        FileNode realNode = readwriteCopy.get(fileUuid);
        copy(realNode, fileNode);
        readwriteCopy.remove(fileUuid);

        writeToDisk();
    }

    public void copy(FileNode realNode, FileNode copyNode) {
        realNode.getValue(copyNode);
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        DirNode dirNode = null;
        if(fileUri.indexOf('/', 1) == -1) {
            dirNode = rootNode;
        } else {
            dirNode = (DirNode) parseUri(rootNode, fileUri.substring(0, fileUri.lastIndexOf('/')));
        }
        String name = fileUri.substring(fileUri.lastIndexOf('/') + 1, fileUri.length());
        if(dirNode.findByName(name) != null) throw new FileAlreadyExistsException("dir already exist");
        DirNode node = new DirNode();
        dirNode.addEntry(new Entry(name, node));
        writeToDisk();
    }

    @Override
    public LocatedBlock addBlock(UUID fileUuid) {
        FileNode fileNode = readwritePFile.get(fileUuid);
        if(fileNode == null) return null;

        Iterator<String> iterator = dataNodes.keySet().iterator();
        String host = iterator.next();
        BitSet bitSet = dataNodes.get(host);
        int blockNumber = -1;
        for(int i = 0; i < bitSet.size(); i ++) {
            if(!bitSet.get(i)) {
                blockNumber = i;
                break;
            }
        }
        if(blockNumber == -1) {
            blockNumber = bitSet.size();
        }
        bitSet.set(blockNumber, true);
        LocatedBlock locatedBlock = null;
        try {
            locatedBlock = new LocatedBlock(InetAddress.getByName(host), blockNumber);
        } catch (Exception e) {
            e.printStackTrace();
        }
        writeToDisk(host);

        BlockInfo blockInfo = new BlockInfo();
        blockInfo.addLocatedBlock(locatedBlock);
        fileNode.addBlockInfo(blockInfo);
        return locatedBlock;
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
        List<LocatedBlock> list = new ArrayList<>();
        for(int i = 0; i < blockAmount; i ++) {
            LocatedBlock locatedBlock = addBlock(fileUuid);
            list.add(locatedBlock);
        }
        return list;
    }

    @Override
    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
        FileNode fileNode = readwritePFile.get(fileUuid);
        if(fileNode == null) return;
        BlockInfo blockInfo = fileNode.removeLastBlockInfo();
        Iterator<LocatedBlock> iterator = blockInfo.iterator();
        while(iterator.hasNext()) {
            LocatedBlock locatedBlock = iterator.next();
            BitSet bitSet = dataNodes.get(locatedBlock.getInetAddress().getHostName());
            bitSet.set(locatedBlock.getBlockNumber(), false);
            writeToDisk(locatedBlock.getInetAddress().getHostName());
        }
    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        for(int i = 0; i < blockAmount; i ++) {
            removeLastBlock(fileUuid);
        }
    }

    public void newDataNode(String host) {
        if(dataNodes.containsKey(host)) return;
        dataNodes.put(host, new BitSet(32));
        writeToDisk(host);
    }

    protected Node parseUri(DirNode dirNode, String fileUri) throws FileNotFoundException {
        if(fileUri.indexOf('/', 1) == -1) {
            String name = fileUri.substring(1, fileUri.length());
            return dirNode.findByName(name);
        }

        String name = fileUri.substring(1, fileUri.indexOf('/', 1));
        String nextDir = fileUri.substring(fileUri.indexOf('/', 1), fileUri.length());
        Node node = dirNode.findByName(name);
        if(node instanceof FileNode || node == null) throw new FileNotFoundException();
        return parseUri((DirNode) node, nextDir);
    }

    public void writeToDisk(String host) {
        BitSet bitSet = dataNodes.get(host);
        String filename = path + host + ".info";
        File file = new File(filename);
        try {
            if(!file.exists()) {
                file.createNewFile();
            }
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
            oos.writeObject(bitSet);
            oos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void writeToDisk() {
        String filename = path + "fileTree.node";
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename));
            oos.writeObject(rootNode);
            oos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
