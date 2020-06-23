package org.apache.kafka.clients;

import com.ibm.disni.util.MemoryUtils;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSge;
import org.apache.kafka.clients.producer.internals.RdmaSessionHandlers;

import java.nio.ByteBuffer;
import java.util.LinkedList;

public class ProduceAtomicFetchRDMAWriteRequest implements RDMAWrBuilder  {

    private long remoteAddress;
    private int rkey;

    private long localAddress;
    private int lkey;
    private ByteBuffer buf;
    private long addval;



    public ProduceAtomicFetchRDMAWriteRequest(long remoteAddress, int rkey, long localAddress, int lkey, ByteBuffer buf, long addval){
        this.remoteAddress = remoteAddress;
        this.rkey = rkey;
        this.localAddress = localAddress;
        this.lkey = lkey;
        this.buf = buf;
        this.addval = addval;
    }

    public long GetAdd(){
        return addval;
    }

    @Override
    public LinkedList<IbvSendWR> build() {
        LinkedList<IbvSendWR> wrs = new LinkedList<>();

        IbvSge sgeSend = new IbvSge();
        sgeSend.setAddr(localAddress);
        sgeSend.setLength(8);
        sgeSend.setLkey(lkey);
        LinkedList<IbvSge> sgeList = new LinkedList<>();
        sgeList.add(sgeSend);


        IbvSendWR sendWR = new IbvSendWR();
       // sendWR.setWr_id(9999);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_ATOMIC_FETCH_AND_ADD);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        IbvSendWR.Atomic rdmapart = sendWR.getAtomic();
        rdmapart.setRemote_addr(remoteAddress);
        rdmapart.setCompare_add(addval);
        rdmapart.setRkey(rkey);

        wrs.add(sendWR);
        return wrs;
    }

    @Override
    public ByteBuffer getTargetBuffer() {
        buf.position(0);
        return buf;
    }
}
