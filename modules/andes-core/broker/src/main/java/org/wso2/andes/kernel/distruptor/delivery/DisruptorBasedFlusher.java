/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.distruptor.delivery;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.LocalSubscription;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Disruptor based message flusher. This use a ring buffer to deliver message to subscribers
 */
public class DisruptorBasedFlusher {

    /**
     * Disruptor instance used in the flusher
     */
    private final Disruptor<DeliveryEventData> disruptor;

    /**
     * Ring buffer used for delivery
     */
    private final RingBuffer<DeliveryEventData> ringBuffer;

    public DisruptorBasedFlusher() {
        Integer ringBufferSize = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_RING_BUFFER_SIZE);
        Integer parallelContentReaders = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_PARALLEL_CONTENT_READERS);
        Integer parallelDeliveryHandlers = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_PARALLEL_DELIVERY_HANDLERS);

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("DisruptorBasedFlusher-%d").build();
        Executor threadPoolExecutor = Executors.newCachedThreadPool(namedThreadFactory);

        disruptor = new Disruptor<DeliveryEventData>(new DeliveryEventData.DeliveryEventDataFactory(), ringBufferSize,
                                                     threadPoolExecutor,
                                                     ProducerType.MULTI,
                                                     new BlockingWaitStrategy());

        disruptor.handleExceptionsWith(new DeliveryExceptionHandler());

        // Initialize content readers
        ContentCacheCreator[] contentReaders = new ContentCacheCreator[parallelContentReaders];
        for (int i = 0; i < parallelContentReaders; i++) {
            contentReaders[i] = new ContentCacheCreator(i, parallelContentReaders);
        }

        // Initialize delivery handlers
        DeliveryEventHandler[] deliveryEventHandlers = new DeliveryEventHandler[parallelDeliveryHandlers];
        for (int i = 0; i < parallelDeliveryHandlers; i++) {
            deliveryEventHandlers[i] = new DeliveryEventHandler(i, parallelDeliveryHandlers);
        }

        disruptor.handleEventsWith(contentReaders).then(deliveryEventHandlers);

        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }

    /**
     * Submit a delivery task to the flusher
     *
     * @param subscription
     *         Local subscription
     * @param metadata
     *         Message metadata
     */
    public void submit(LocalSubscription subscription, AndesMessageMetadata metadata) {
        long nextSequence = ringBuffer.next();

        // Initialize event data holder
        DeliveryEventData data = ringBuffer.get(nextSequence);
        data.setLocalSubscription(subscription);
        data.setMetadata(metadata);

        ringBuffer.publish(nextSequence);
    }

    /**
     * Waits until all events currently in the disruptor have been processed by all event processors
     * and then halts the processors. It is critical that publishing to the ring buffer has stopped
     * before calling this method, otherwise it may never return.
     */
    public void stop() {
        disruptor.shutdown();
    }
}
