/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.protocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

public interface IDataNodeProtocol {
    /**
     * Read data from a block.
     * It should be redirect to [blockNumber].block file
     *
     * @param fileUuid    the file uuid to check whether have permission to read or not. Put off to future lab.
     * @param blockNumber the block number to be read
     * @param offset      the offset on the block file
     * @param size        the total size to be read
     * @return the total number of bytes read into the buffer
     * @throws IndexOutOfBoundsException if offset less than zero, or offset+size larger than block size.
     * @throws FileNotFoundException     if the block is free (block file not exist)
     * @throws IllegalStateException     if uuid is illegal or has no permission on this file
     */
    byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IllegalStateException, IndexOutOfBoundsException, IOException;

    /**
     * Write data to a block.
     * It should be redirect to [blockNumber].block file
     *
     * @param fileUuid    the file uuid to check whether have permission to write or not. Put off to future lab.
     * @param blockNumber the block number to be written
     * @param offset      the offset on the block file
     * @param b           the buffer that stores the data
     * @throws IndexOutOfBoundsException if offset less than zero, or offset+size larger than block size.
     * @throws IllegalStateException     if uuid is illegal or has no permission on this file
     */
    void write(UUID fileUuid, int blockNumber, int offset, byte b[]) throws IllegalStateException, IndexOutOfBoundsException, IOException;

//    put off due to its difficulties
//    /**
//     * Copy data from originBlock and then write data to a block.
//     * It should be redirect to [blockNumber].block file
//     *
//     * @param fileUuid          the file uuid to check whether have permission to write or not and check read permission to originalBlockNumber
//     * @param originBlockNumber the original block number to be copy
//     * @param blockNumber       the block number to be written
//     * @param offset            the offset on the block file
//     * @param b                 the buffer that stores the data
//     * @throws IndexOutOfBoundsException if offset less than zero, or offset+size larger than block size.
//     */
//    void copyOnWrite(UUID fileUuid, int originBlockNumber, int blockNumber, int offset, byte b[]) throws IndexOutOfBoundsException, IOException;
}