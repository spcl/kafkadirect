/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.producer.internals;

import com.ibm.disni.util.MemoryUtils;
import org.apache.kafka.clients.ProduceAtomicFetchRDMAWriteRequest;
import org.apache.kafka.clients.ProduceRDMAWriteRequest;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.RDMAProduceAddressResponse;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;
import java.util.Map;


public class RdmaSessionHandlers {


    private final RdmaBufferPool pool;
    private final long tcptimeout;


    /**
     * All of the partitions which exist in the fetch request session.
     */
    private LinkedHashMap<TopicPartition, ProduceRdmaRequestData> sessionPartitions =
            new LinkedHashMap<>(0);


    public RdmaSessionHandlers(RdmaBufferPool pool, long tcptimeout) {

        this.pool = pool;
        this.tcptimeout = tcptimeout;

    }

    public boolean requiresAddressUpdate(TopicPartition tp, long nowMs) {
        if (!sessionPartitions.containsKey(tp)) {
            sessionPartitions.put(tp, new ProduceRdmaRequestData(tp, pool, nowMs, this.tcptimeout));
            return true;
        }
        return sessionPartitions.get(tp).requiresAddressUpdate(nowMs);
    }


    public boolean isReady(TopicPartition tp) {
        return sessionPartitions.get(tp).isReady();
    }

    public boolean fitsBatch(TopicPartition tp, ProducerBatch batch) {
        ProduceRdmaRequestData data = sessionPartitions.get(tp);
        return data.fitsBatch(batch);
    }


    public boolean canSendNewFileRequest(TopicPartition tp, long timeMs) {
        ProduceRdmaRequestData data = sessionPartitions.get(tp);
        return data.canSendNewFileRequest(timeMs);
    }


    public ProduceRDMAWriteRequest createRequest(TopicPartition tp, ProducerBatch batch) {
        ProduceRdmaRequestData data = sessionPartitions.get(tp);
        return data.createRequest(batch);
    }

    public void updateAddresses(Map<TopicPartition, RDMAProduceAddressResponse.PartitionResponse> data) {
        for (Map.Entry<TopicPartition, RDMAProduceAddressResponse.PartitionResponse> entry : data.entrySet()) {
            TopicPartition tp = entry.getKey();
            RDMAProduceAddressResponse.PartitionResponse updateData = entry.getValue();
            sessionPartitions.get(tp).update(updateData);
        }
    }



    public static class ProduceRdmaRequestData {
        public final TopicPartition topicPartition;
        private final RdmaBufferPool pool;
        private final long tcptimeout;

        private long lastUpdateRequested;

        private long startAddress;
        private long offsetAddress; // I use long to detect overflow
        private long lastAddress;

        private long baseOffset;
        private int rkey;
        private int immdata;


        private long slotAddress;
        private int slotRkey;


        ByteBuffer localSlot;
        private long localSlotAddress;
        private int localSlotLkey;

        boolean hasPending = false;



        ProduceRdmaRequestData(TopicPartition topicPartition, RdmaBufferPool pool, long nowMs, long tcptimeout) {
            this.topicPartition = topicPartition;
            this.pool = pool;
            this.lastUpdateRequested = nowMs;
            this.startAddress = -1;
            this.offsetAddress = -1;
            this.lastAddress = -1;
            this.rkey = -1;
            this.immdata = -1;
            this.baseOffset = -1L;
            this.tcptimeout = tcptimeout;
            this.localSlot= null;
        }


        boolean requiresAddressUpdate(long nowMs) {
            if (baseOffset == -1L  && nowMs - lastUpdateRequested > tcptimeout) {
                lastUpdateRequested = nowMs;
                return true;
            } else {
                return false;
            }
        }

        boolean isReady() {
            return (baseOffset != -1L && !hasPending);
        }

        public void update(RDMAProduceAddressResponse.PartitionResponse data)   {
            if (data.baseOffset == this.baseOffset) {
                System.out.println("Received the same metadata twice");
                return;
            }
            this.startAddress = data.address;
            this.offsetAddress  = data.addrPositionOffset;
            this.lastAddress = data.address + data.length;
            this.rkey = data.rkey;
            this.immdata = data.immdata;
            this.baseOffset = data.baseOffset;



            // for shared access
            this.slotAddress = data.slotAddress;
            this.slotRkey = data.slotRkey;

            if(this.slotAddress != 0 && localSlot==null){
                try {
                    this.localSlot = pool.allocateSlot();
                } catch (Exception exp){
                    System.out.printf("Error on allocate slot\n");
                }
                this.localSlot.order(ByteOrder.LITTLE_ENDIAN);
                localSlotAddress = MemoryUtils.getAddress(localSlot);
                localSlotLkey = pool.getSlotLkey();
            }
        }

        public boolean fitsBatch(ProducerBatch batch) {
            int size = batch.estimatedSizeInBytes();
            return fitsBatch(size);
        }

        public boolean canSendNewFileRequest(long nowMs) {
            if (nowMs - lastUpdateRequested > tcptimeout) {
                lastUpdateRequested = nowMs;
                return true;
            } else {
                return false;
            }
        }

        protected boolean fitsBatch(int size) {
            return (lastAddress - startAddress - offsetAddress) >= size;
            //return (lastAddress - startAddress - offsetAddress) != 0; // for debugging
        }

        public boolean setOffset(long offset,int size){
            if(offset < offsetAddress){
                System.out.println("FAA Offset is smaller because of overflow");
                return false;
            }
            if(offset + size > (lastAddress - startAddress )){
                offsetAddress = (lastAddress - startAddress); // to force be full
                return false;
            }
            offsetAddress = (offset + size);
            return true;
        }

        public ProduceRDMAWriteRequest createRequest(ProducerBatch batch) {

            int lkey = pool.getLkey(batch.buffer());
            int size = batch.estimatedSizeInBytes();
            assert fitsBatch(size);
            ProduceRDMAWriteRequest request = new ProduceRDMAWriteRequest(batch, baseOffset, startAddress, rkey, lkey, immdata,this);

            if(this.slotAddress == 0) {
                request.incrementAddress((int)offsetAddress);
                offsetAddress += size;
            }  else{
                hasPending = true;
                long addval = ((1L << 40) +  size);
                ProduceAtomicFetchRDMAWriteRequest prereq = new ProduceAtomicFetchRDMAWriteRequest(slotAddress, slotRkey,localSlotAddress,localSlotLkey,localSlot,addval);
                request.setAtomicPreop(prereq);
            }
            return request;
        }



    }



}
