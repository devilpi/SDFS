/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.protocol;

//Put off to future lab due to its difficulties
public interface INameNodeDataNodeProtocol {
//    /**
//     * Check uuid have permission to read on this block
//     *
//     * @param uuid        file access uuid
//     * @param blockNumber block that require to read
//     * @return true if uuid have permission, otherwise false
//     */
//    boolean checkUuidReadable(UUID uuid, int blockNumber);
//
//    /**
//     * Check uuid have permission to write on this block
//     *
//     * @param uuid        file access uuid
//     * @param blockNumber block that require to write
//     * @return true if uuid have permission, otherwise false
//     */
//    boolean checkUuidWritable(UUID uuid, int blockNumber);
//
//    /**
//     * Get get replication work for this datanode as well as a heart beat to the datanode
//     * It should be trigger every 1 minutes
//     * If namenode do not receive heartbeat for 5 mintues,
//     * it should treat datanode have been downed and need to replicate its data (Future lab)
//     *
//     * @return ReplicationWork should be done by this datanode
//     */
//    ReplicationWork getReplicationWork();
//
//    /**
//     * Confirm that last replication work with replicationWorkUuid has been done successful
//     *
//     * @param replicationWorkUuid The ReplicationWork has been done
//     */
//    void commitReplicationWork(UUID replicationWorkUuid);
}
