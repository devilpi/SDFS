package sdfs.client;

import sdfs.namenode.NameNode;
import sdfs.namenode.SDFSFileChannel;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by devilpi on 26/11/2017.
 */
public class Client {
    public static void main(String[] args) {
        SimpleDistributedFileSystem fileSystem = new SimpleDistributedFileSystem(InetSocketAddress.createUnresolved("localhost", NameNode.NAME_NODE_PORT), 16);

        try {
            if (args[0].equals("put")) {

                String sdfsURI = args[2].substring(args[2].indexOf("/", args[2].lastIndexOf(":")), args[2].length());
                String localURI = args[1];
                SDFSFileChannel channel = fileSystem.create(sdfsURI);

                FileChannel fileChannel = new FileInputStream(localURI).getChannel();
                ByteBuffer buffer = ByteBuffer.allocate(1024);
                while (fileChannel.read(buffer) != -1) {
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();
                }
                fileChannel.close();
                channel.close();

            } else if (args[0].equals("get")) {

                String sdfsURI = args[2].substring(args[2].indexOf("/", args[2].lastIndexOf(":")), args[2].length());
                String localURI = args[1];
                SDFSFileChannel channel = fileSystem.openReadonly(sdfsURI);

                FileChannel fileChannel = new FileOutputStream(localURI).getChannel();
                ByteBuffer buffer = ByteBuffer.allocate(1024);
                while (channel.read(buffer) > 0) {
                    buffer.flip();
                    fileChannel.write(buffer);
                    buffer.clear();
                }
                fileChannel.close();
                channel.close();

            } else if (args[0].equals("mkdir")) {

                String sdfsURI = args[1].substring(args[1].indexOf("/", args[1].lastIndexOf(":")), args[1].length());
                fileSystem.mkdir(sdfsURI);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
