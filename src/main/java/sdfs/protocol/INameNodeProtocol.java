/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.protocol;

import sdfs.namenode.LocatedBlock;
import sdfs.namenode.SDFSFileChannel;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface INameNodeProtocol {
    /**
     * Open a readonly file that is already exist.
     * Allow multi readonly access to the same file.
     * Also, if the file is currently being writing by other client, it is also LEGAL to open the same file. However, only after the write instance is closed could other client to read the new data.
     *
     * @param fileUri The file uri to be open
     * @return The SDFSFileChannel represent the file
     * @throws FileNotFoundException if the file is not exist
     */
    SDFSFileChannel openReadonly(String fileUri) throws IOException;

    /**
     * Open a readwrite file that is already exist.
     * At most one UUID with readwrite permission could exist on the same file at the same time.
     *
     * @param fileUri The file uri to be open
     * @return The SDFSFileChannel represent the file
     * @throws FileNotFoundException if the file is not exist
     * @throws IllegalStateException if the file is already opened readwrite
     */
    SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException, IOException;

    /**
     * Create a empty file. It should maintain a readwrite file on the memory and return the uuid to client.
     *
     * @param fileUri The file uri to be create
     * @return The SDFSFileChannel represent the file.
     * @throws FileAlreadyExistsException if the file is already exist
     */
    SDFSFileChannel create(String fileUri) throws IllegalStateException, IOException;

    /**
     * Close a readonly file.
     *
     * @param fileUuid file to be closed
     * @throws IllegalStateException if uuid is illegal
     */
    void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException;

    /**
     * Close a readwrite file. If file metadata has been changed, store them on the disk.
     *
     * @param fileUuid    file to be closed
     * @param newFileSize The new file size after modify
     * @throws IllegalArgumentException if new file size not in (blockAmount * BLOCK_SIZE, (blockAmount + 1) * BLOCK_SIZE]
     * @throws IllegalStateException    if uuid is illegal
     */
    void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException;

    /**
     * Make a directory on given file uri.
     *
     * @param fileUri the directory path
     * @throws FileAlreadyExistsException if directory or file is already exist
     */
    void mkdir(String fileUri) throws IOException;

    /**
     * Request a new free block for a file
     * No metadata should be written to disk until it is correctly close
     *
     * @param fileUuid the file uuid with readwrite state
     * @return a block that is free and could be used by client
     * @throws IllegalStateException if file is readonly
     */
    LocatedBlock addBlock(UUID fileUuid) throws IllegalStateException;

    /**
     * Request a special amount of free blocks for a file
     * No metadata should be written to disk until it is correctly close
     *
     * @param fileUuid    the file uuid with readwrite state
     * @param blockAmount the request block amount
     * @return a special amount of blocks that is free and could be used by client
     * @throws IllegalStateException if file is readonly
     */
    List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException;

    /**
     * Delete the last block for a file
     * No metadata should be written to disk until it is correctly close
     *
     * @param fileUuid the file uuid with readwrite state
     * @throws IllegalStateException if there is no block in this file
     */
    void removeLastBlock(UUID fileUuid) throws IllegalStateException;

    /**
     * Delete the last blocks for a file
     * No metadata should be written to disk until it is correctly close
     *
     * @param fileUuid    the file uuid with readwrite state
     * @param blockAmount the blocks amount to be removed
     * @throws IllegalStateException if there is no enough block in this file
     */
    void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException;
}
