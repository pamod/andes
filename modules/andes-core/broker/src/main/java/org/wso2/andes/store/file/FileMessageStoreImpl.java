/*
 * Copyright (c) 2005-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.store.file;

import com.gs.collections.api.iterator.MutableLongIterator;
import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.apache.log4j.Logger;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.dtx.XidImpl;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.DtxStore;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.dtx.AndesPreparedMessageMetadata;
import org.wso2.andes.kernel.dtx.DtxBranch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.Xid;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;


/**
 * Will represent a capability to store the message into a file
 */
public class FileMessageStoreImpl implements MessageStore {

    private static final Logger log = Logger.getLogger(FileMessageStoreImpl.class);

    /**
     * Instance which will
     */
    private DB brokerStore;

    /**
     * tmp //TODO remove this mechanism
     */
    private int messageIdCount = 0;


    @Override
    public boolean isOperational(String testString, long testTime) {
        return false;
    }

    @Override
    public DurableStoreConnection initializeMessageStore(AndesContextStore contextStore, ConfigurationProperties
            connectionProperties) throws AndesException {
        try {
            int kb = 1048576;
            Options options = new Options();
            options.cacheSize(1024 * kb);
            //Number of keys will be defined through the buffer size
            options.writeBufferSize(512 * kb);
            options.createIfMissing(true);
            brokerStore = factory.open(new File("mbstore"), options);
        } catch (IOException e) {
            throw new AndesException("Error occurred while initializing the store ", e);
        }
        //We currently will not consider using the returned value
        return null;
    }

    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {

    }

    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        return null;
    }

    @Override
    public LongObjectHashMap<List<AndesMessagePart>> getContent(LongArrayList messageIDList) throws AndesException {

        long currentTime = System.currentTimeMillis();
        int numberOfMessages = messageIDList.size();

        MutableLongIterator mutableLongIterator = messageIDList.longIterator();
        List<AndesMessagePart> messageContentList = new ArrayList<>();
        LongObjectHashMap<List<AndesMessagePart>> messages = new LongObjectHashMap<>();

        while (mutableLongIterator.hasNext()) {
            long messageId = mutableLongIterator.next();
            String contentIdentifier = FileStoreConstants.TBL_MESSAGE_CONTENT + "|" + messageId;
            byte[] messageContent = brokerStore.get(contentIdentifier.getBytes());
            AndesMessagePart messagePart = new AndesMessagePart();
            messagePart.setData(messageContent);
            messagePart.setMessageID(messageId);

            messageContentList.add(messagePart);

            messages.put(messageId, messageContentList);
        }

        long processedTime = System.currentTimeMillis();

        log.info("Total Time to Retrieve Content For "+numberOfMessages+" took "+(processedTime-currentTime)+"ms");

        return messages;
    }

    @Override
    public void storeMessages(List<AndesMessage> messageList) throws AndesException {

        long currentTime = System.currentTimeMillis();
        int numberOfMessages = messageList.size();

        WriteBatch batch = brokerStore.createWriteBatch();
        try {
            //Need to implement this
            for (AndesMessage message : messageList) {
                AndesMessageMetadata metadata = message.getMetadata();
                List<AndesMessagePart> contentChunkList = message.getContentChunkList();
                byte[] metaDataContent = metadata.getMetadata();
                String destination = metadata.getDestination();
                long messageID = metadata.getMessageID();
                //long messageID = 1;
                //TODO remove this
                messageID = messageIdCount++;
                String metaDataIdentifier = FileStoreConstants.TBL_MESSAGE_METADATA_PFX + "|" + destination + "|" +
                        messageID;
                String contentIdentifier = FileStoreConstants.TBL_MESSAGE_CONTENT + "|" + messageID;
                brokerStore.put(metaDataIdentifier.getBytes(), metaDataContent);
                byte[] data = new byte[10000];

                for (AndesMessagePart messageContentChunk : contentChunkList) {
                    data = messageContentChunk.getData();
                }

                //TODO change this and do a proper byte copy of the content
                batch.put(contentIdentifier.getBytes(), data);
            }

            brokerStore.write(batch);
        }finally {
            try {
                batch.close();
            } catch (IOException e) {
                log.error("Error occured while closing ",e);
            }
        }

        long processedTime = System.currentTimeMillis();
        log.info("Total Time to Store Message For "+numberOfMessages+" took "+(processedTime-currentTime)+"ms");
    }

    @Override
    public void moveMetadataToQueue(long messageId, String currentQueueName, String targetQueueName) throws
            AndesException {

    }

    @Override
    public void moveMetadataToDLC(long messageId, String dlcQueueName) throws AndesException {

    }

    @Override
    public void moveMetadataToDLC(List<AndesMessageMetadata> messages, String dlcQueueName) throws AndesException {

    }

    @Override
    public void updateMetadataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws
            AndesException {

    }

    @Override
    public AndesMessageMetadata getMetadata(long messageId) throws AndesException {
        return null;
    }

    @Override
    public List<DeliverableAndesMetadata> getMetadataList(String storageQueueName, long firstMsgId, long lastMsgID)
            throws AndesException {

        long currentTime = System.currentTimeMillis();

        List<DeliverableAndesMetadata> metadataList = new ArrayList<>();

        String metaDataIdentifier = FileStoreConstants.TBL_MESSAGE_METADATA_PFX + "|" + storageQueueName + "|" +
                firstMsgId;
        byte[] bytes = brokerStore.get(metaDataIdentifier.getBytes());
        if (null != bytes) {
            DeliverableAndesMetadata metadata = new DeliverableAndesMetadata(firstMsgId, bytes, true);
            metadata.setStorageQueueName(storageQueueName);
            metadataList.add(metadata);
        }

        long processedTime = System.currentTimeMillis();

        if (log.isDebugEnabled()) {
            log.debug("Total Time to retrieve meta data took "+(processedTime-currentTime)+"ms");
        }

        return metadataList;
    }

    @Override
    public long getMessageCountForQueueInRange(String storageQueueName, long firstMessageId, long lastMessageId)
            throws AndesException {
        return 0;
    }

    @Override
    public List<AndesMessageMetadata> getMetadataList(String storageQueueName, long firstMsgId, int count) throws
            AndesException {
        return null;
    }

    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataForQueueFromDLC(String storageQueueName, String
            dlcQueueName, long firstMsgId, int count) throws AndesException {
        return null;
    }

    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromDLC(String dlcQueueName, long firstMsgId, int count)
            throws AndesException {
        return null;
    }

    @Override
    public void deleteMessageMetadataFromQueue(String storageQueueName, List<AndesMessageMetadata> messagesToRemove)
            throws AndesException {

    }

    @Override
    public void deleteMessages(Collection<? extends AndesMessageMetadata> messagesToRemove) throws AndesException {
        for(AndesMessageMetadata metadata : messagesToRemove){
            long messageID = metadata.getMessageID();
            String storageQueueName = metadata.getStorageQueueName();
            String metaDataIdentifier = FileStoreConstants.TBL_MESSAGE_METADATA_PFX + "|" + storageQueueName + "|" +
                    messageID;
            String contentIdentifier = FileStoreConstants.TBL_MESSAGE_CONTENT + "|" + messageID;
            brokerStore.delete(metaDataIdentifier.getBytes());
            brokerStore.delete(contentIdentifier.getBytes());
        }
        //Need to impliment this
    }

    @Override
    public void deleteMessages(List<Long> messagesToRemove) throws AndesException {
        for (Long messageID : messagesToRemove) {
            String metaDataIdentifier = FileStoreConstants.TBL_MESSAGE_METADATA_PFX + "|" + "queue2" + "|" +
                    messageID;
            String contentIdentifier = FileStoreConstants.TBL_MESSAGE_CONTENT + "|" + "queue2" + "|" + messageID;
            brokerStore.delete(metaDataIdentifier.getBytes());
            brokerStore.delete(contentIdentifier.getBytes());
        }
    }

    @Override
    public void deleteDLCMessages(List<AndesMessageMetadata> messagesToRemove) throws AndesException {

    }

    @Override
    public List<Long> getExpiredMessages(long lowerBoundMessageID, String queueName) throws AndesException {
        return null;
    }

    @Override
    public List<Long> getExpiredMessagesFromDLC(long messageCount) throws AndesException {
        return null;
    }

    @Override
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic, String
            destination) throws AndesException {

    }

    @Override
    public int deleteAllMessageMetadata(String storageQueueName) throws AndesException {
        return 0;
    }

    @Override
    public int clearDLCQueue(String dlcQueueName) throws AndesException {
        return 0;
    }

    @Override
    public LongArrayList getMessageIDsAddressedToQueue(String storageQueueName, Long startMessageID) throws
            AndesException {
        return null;
    }

    @Override
    public void addQueue(String storageQueueName) throws AndesException {

    }

    @Override
    public Map<String, Integer> getMessageCountForAllQueues(List<String> queueNames) throws AndesException {
        return null;
    }

    @Override
    public long getMessageCountForQueue(String storageQueueName) throws AndesException {
        return 0;
    }

    @Override
    public long getApproximateQueueMessageCount(String storageQueueName) throws AndesException {
        return 0;
    }

    @Override
    public long getMessageCountForQueueInDLC(String storageQueueName, String dlcQueueName) throws AndesException {
        return 0;
    }

    @Override
    public long getMessageCountForDLCQueue(String dlcQueueName) throws AndesException {
        return 0;
    }

    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {

    }

    @Override
    public void removeQueue(String storageQueueName) throws AndesException {

    }

    @Override
    public void removeLocalQueueData(String storageQueueName) {

    }

    @Override
    public void incrementMessageCountForQueue(String storageQueueName, long incrementBy) throws AndesException {

    }

    @Override
    public void decrementMessageCountForQueue(String storageQueueName, long decrementBy) throws AndesException {

    }

    @Override
    public void storeRetainedMessages(Map<String, AndesMessage> retainMap) throws AndesException {

    }

    @Override
    public List<String> getAllRetainedTopics() throws AndesException {
        return null;
    }

    @Override
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException {
        return null;
    }

    @Override
    public DeliverableAndesMetadata getRetainedMetadata(String destination) throws AndesException {
        return null;
    }

    @Override
    public List<Long> getMessageIdsInDLCForQueue(String sourceQueueName, String dlcQueueName, long startMessageId,
                                                 int messageLimit) throws AndesException {
        return null;
    }

    @Override
    public List<Long> getMessageIdsInDLC(String dlcQueueName, long startMessageId, int messageLimit) throws
            AndesException {
        return null;
    }

    @Override
    public void close() {
        try {
            brokerStore.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public DtxStore getDtxStore() {
        //Need to call this
        return new DtxStore() {
            @Override
            public long storeDtxRecords(Xid xid, List<AndesMessage> enqueueRecords, List<? extends
                    AndesMessageMetadata> dequeueRecords) throws AndesException {
                return 0;
            }

            @Override
            public void updateOnCommit(long internalXid, List<AndesMessage> enqueueRecords) throws AndesException {

            }

            @Override
            public void updateOnRollback(long internalXid, List<AndesPreparedMessageMetadata> messagesToRestore)
                    throws AndesException {

            }

            @Override
            public long recoverBranchData(DtxBranch branch, String nodeId) throws AndesException {
                return 0;
            }

            @Override
            public Set<XidImpl> getStoredXidSet(String nodeId) throws AndesException {
                return new HashSet<>();
            }

            @Override
            public boolean isOperational(String testString, long testTime) {
                return false;
            }
        };
    }
}
