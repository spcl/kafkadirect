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
package org.apache.kafka.clients;

import com.ibm.disni.util.MemoryUtils;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSendWR.Rdma;
import com.ibm.disni.verbs.IbvSge;
import org.apache.kafka.clients.producer.internals.ProducerBatch;
import org.apache.kafka.clients.producer.internals.RdmaSessionHandlers;
import org.apache.kafka.common.record.MemoryRecords;

import java.nio.ByteBuffer;
import java.util.LinkedList;


public class ProduceRDMAWriteRequest implements RDMAWrBuilder {

    private long remoteAddress;
    private int rkey;
    private int length;

    private ByteBuffer targetBuffer;
    private int lkey;
    private int immdata;

    private final ProducerBatch batch;
    public final long baseOffset;

    public ProduceAtomicFetchRDMAWriteRequest prereq;
    final public RdmaSessionHandlers.ProduceRdmaRequestData owner;

    public ProduceRDMAWriteRequest(ProducerBatch batch, long baseOffset, long remoteAddress, int rkey,  int lkey, int immdata, RdmaSessionHandlers.ProduceRdmaRequestData owner) {
        this.batch = batch;
        this.remoteAddress = remoteAddress;
        this.rkey = rkey;
        this.length = batch.records().sizeInBytes();
        this.targetBuffer = batch.records().buffer();
        this.lkey = lkey;
        this.immdata = immdata;
        this.baseOffset = baseOffset;
        this.prereq = null;
        this.owner = owner;
    }


    public ProduceRDMAWriteRequest(long remoteAddress, int rkey, int length, ByteBuffer targetBuffer, int lkey, int immdata) {
        this.batch = null;
        this.remoteAddress = remoteAddress;
        this.rkey = rkey;
        this.length = length;
        this.targetBuffer = targetBuffer;
        this.lkey = lkey;
        this.immdata = immdata;
        this.baseOffset = 0;
        this.prereq = null;
        this.owner = null;
    }

    public ProducerBatch getBatch() {
        return batch;
    }

    public void setAtomicPreop(ProduceAtomicFetchRDMAWriteRequest prereq ){
        this.prereq = prereq;
    }

    public boolean hasAtomicPreop( ){
        return this.prereq != null;
    }


    public void incrementAddress(int offset){
        remoteAddress+=offset;
    }

    public void updateBuffer(MemoryRecords records) {
        this.targetBuffer = records.buffer();
        this.length = records.sizeInBytes();
    }

    @Override
    public LinkedList<IbvSendWR> build() {
        LinkedList<IbvSendWR> wrs = new LinkedList<>();

        IbvSge sgeSend = new IbvSge();
        sgeSend.setAddr(MemoryUtils.getAddress(targetBuffer));
        sgeSend.setLength(length);
        sgeSend.setLkey(lkey);
        LinkedList<IbvSge> sgeList = new LinkedList<>();
        sgeList.add(sgeSend);


        IbvSendWR sendWR = new IbvSendWR();
        //sendWR.setWr_id(1002);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_WRITE_WITH_IMM);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.setImm_data(immdata);

        Rdma rdmapart = sendWR.getRdma();
        rdmapart.setRemote_addr(remoteAddress);
        rdmapart.setRkey(rkey);

        wrs.add(sendWR);

        return wrs;
    }

    @Override
    public ByteBuffer getTargetBuffer() {
        return targetBuffer;
    }
}
