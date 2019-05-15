#include <iostream>
#include <algorithm>
#include <iterator>

#include <seastar/net/rdma.hh>

#include <infiniband/verbs.h>


namespace seastar {

namespace rdma {

static bool initialized = false;
static struct ibv_context* ctx = nullptr;

void RDMAConnection::handshakeRequest(uint32_t remoteQP) {
    // TODO move to slow core
    struct ibv_qp_init_attr init_attr = {
        .qp_context = nullptr,
        .send_cq = stack->RCCQ,
        .recv_cq = stack->RCCQ,
        .srq = stack->SRQ,
        .cap = {
            .max_send_wr = SendWRData::maxWR,
            .max_recv_wr = RecvWRData::maxWR,
            .max_send_sge = 1,
            .max_recv_sge = 1,
            .max_inline_data = 0},
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 0
    };
    QP = ibv_create_qp(stack->protectionDomain, &init_attr);
    if (!QP) {
        std::cout << "failed to create QP" << std::endl;
        return;
    }
    
    // Step 3. Transition QP to INIT state
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = 1;
    if (ibv_modify_qp(QP, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
        std::cout << "failed to transition into init state" << std::endl;
        return; 
    }
    // End of slow core part


    
}

void RDMAConnection::handshakeResponse(uint32_t remoteQP) {
    // TODO move this part to slow core
    struct ibv_ah_attr;
    memset(&(driver->AHAttr), 0, sizeof(struct ibv_ah_attr));
    memcpy(AHAttr.grh.dgid.raw, remote.GID.raw, 16);
    AHAttr.grh.sgid_index = 1; // Index 1 is ROCEv2 address
    AHAttr.grh.hop_limit = 64; // Equivalent to IPv4 time to live
    // For ConnectX-4 EN, we have one port per RDMA device, which starts at "1"
    AHAttr.is_global = 1; // Means use GID
    AHAttr.port_num = 1;

    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.ah_attr = AHAttr;
    attr.path_mtu = IBV_MTU_4096;
    attr.dest_qp_num = remoteQP;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 0;
    attr.min_rnr_timer = 2; // 0.02 millisecond
    if (ibv_modify_qp(QP, &attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | 
                                 IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                                 IBV_QP_MIN_RNR_TIMER | IBV_QP_DEST_QPN)) {
        std::cout << "failed to transition into RTR" << std::endl;
        return;
    }

    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 0;
    attr.timeout = 3; // 32 usec
    attr.retry_cnt = 2;
    attr.rnr_retry = 7; // infinite retry
    attr.max_rd_atomic = 0;
    if (ibv_modify_qp(QP, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN | 
                                 IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | 
                                 IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC)) {
        std::cout << "failed to transition into RTS" << std::endl;
        return;
    } 
    // End of slow core part


    isReady = true;
    processSends<std::deque>(sendQueue);
}

template <class VecType>
bool RDMAConnection::processSends(VecType& queue) {
    if (queue.size() == 0 || SendWRs.postedCount == SendWRData::maxWR) {
        return false;
    }

    int toProcess = std::min((int)queue.size(), (int)(SendWRData::maxWR - SendWRs.postedCount));
    int idx = (SendWRs.postedIdx + SendWRs.postedCount) % SendWRData::maxWR;
    int baseIdx = idx;
    struct ibv_send_wr* firstSR = &(SendWRs.SendRequests[idx]);

    for(int i=0; i < toProcess; ++i, idx = (baseIdx + i) % SendWRData::maxWR) {
        temporary_buffer<uint8_t>& sendData = queue[i];
        struct ibv_sge* SG = &(SendWRs.Segments[idx]);
        struct ibv_send_wr* SR = &(SendWRs.SendRequests[idx]);

        SG->addr = (uint64_t)sendData.buffer.get();
        SG->length = sendData.buffer.size();
        SR->sg_list = SG;

        if ((idx+1) % SendWRData::signalThreshold == 0) {
            SR->send_flags = IBV_SEND_SIGNALED;
        } else {
            SR->send_flags = 0;
        }

        if (i == toProcess - 1) {
            SR->next = nullptr;
        } else {
            SR->next = &(SendWRs.SendRequests[(baseIdx+i+1)%SendWRData::maxWR]);
        }

        outstandingBuffers[idx] = std::move(sendData);
    }

    struct ibv_send_wr* badSR;
    if (ibv_post_send(QP, firstSR, &badSR)) {
        // TODO shutdown connection with error
        std::cerr << "Failed to send" << std::endl;
        return false;
    }

    SendWRs.postedCount += toProcess;
    queue.erase(queue.cbegin(), queue.cbegin()+toProcess);
    return true;
} 

future<temporary_buffer<uint8_t>&&> RDMAConnection::recv() {
    // TODO check for connection error state and return exception if needed
    if (recvPromiseActive) {
        return make_exception_future<>();
    }

    if (recvQueue.size()) {
        temporary_buffer<uint8_t> buf = std::move(recvQueue.front());
        recvQueue.pop_front();
        return make_ready_future<>(std::move(buf));
    }

    recvPromise = promise<>();
    recvPromiseActive = true;
    return recvPromise.get_future();
}

void send(std::vector<temporary_buffer<uint8_t>>&& buf) {
    if (!isReady || sendQueue.size()) {
        sendQueue.insert(sendQueue.end(), std::make_move_iterator(buf.begin()), 
                         std::make_move_iterator(buf.end()));
        return;
    }

    processSends<std::vector>(buf);
    if (buf.size()) {
        sendQueue.insert(sendQueue.end(), std::make_move_iterator(buf.begin()), 
                         std::make_move_iterator(buf.end()));
    }
}

int initRDMAContext() {
    if (initialized) {
        std::cerr << "RDMAContext already initialized!" << std::endl;
        return -1;
    }

    int numDevices;
    struct ibv_device** devices = ibv_get_device_list(&numDevices);
    if (!devices) {
        std::cerr << "ibv_get_device_list failed: " << std::strerror(errno) << std::endl;
        return -1;
    }

    // TODO get correct device from configuration
    int deviceIdx = 0;
    for(; deviceIdx < numDevices; ++deviceIdx) {
        if (strcmp(ibv_get_device_name(devices[deviceIdx]), "mlx5_1") == 0) {
            break;
        }
    }
    std::cerr << "RDMA device: " << ibv_get_device_name(devices[deviceIdx]) << std::endl;
    ctx = ibv_open_device(devices[deviceIdx]);
    if (!ctx) {
        std::cerr << "ibv_open_device failed" << std::endl;
        return -1;
    }

    ibv_free_device_list(devices);

    initialized = true;
    return 0;
}

RDMAStack::~RDMAStack() {
    if (UDQP) {
        ibv_destroy_qp(UDQP);
        UDQP = nullptr;
    }

    if (UDCQ) {
        ibv_destroy_cq(UDCQ);
        UDCQ = nullptr;
    }

    for (auto it=AHLookup.begin(); it != AHLookup.end(); ++it) {
        ibv_destroy_ah(it->second);
    }

    if (memRegionHandle) {
        ibv_dereg_mr(memRegionHandle);
        memRegionHandle = nullptr;
    }

    if (protectionDomain) {
        ibv_dealloc_pd(protectionDomain);
        protectionDomain = nullptr;
    }

    // TODO Connections, promises, etc, poller?
}

struct ibv_ah* RDMAStack::makeAH(const union ibv_gid& GID) {
    struct ibv_ah_attr AHAttr;
    memset(&AHAttr, 0, sizeof(struct ibv_ah_attr));
    memcpy(AHAttr.grh.dgid.raw, GID.raw, 16);
    AHAttr.grh.sgid_index = 1; // Index 1 is ROCEv2 address
    AHAttr.grh.hop_limit = 64; // Equivalent to IPv4 time to live
    AHAttr.is_global = 1; // Means use GID
    // For ConnectX-4 EN, we have one port per RDMA device, which starts at "1"
    AHAttr.port_num = 1;

    struct ibv_ah* AH = ibv_create_ah(protectionDomain, &AHAttr);
    if (!AH) {
        // TODO
        std::cerr << "Failed to create AH" << std::endl;
        return nullptr;
    }

    AHLookup[GID] = AH;
    return AH;
}

int RDMAStack::sendUDQPMessage(temporary_buffer<uint8_t> buffer, const union ibv_gid& destGID, uint32_t destQP) {
    if (buffer.size() + 40 > UDQPRxSize) {  
        std::cerr << "Message too large, max size is: " << UDQPRxSize-40 << std::endl;
        assert(false);
        return -1;
    }

    auto AHIt = AHLookup.find(destGID);
    struct ibv_ah* AH = nullptr;
    if (AHIt == AHLookup.end()) {
        // TODO move to slow core
        AH = makeAH(destGID);
    } else {
        AH = AHIt->second;
    }

    int idx = trySendUDQPMessage(buffer, AH, destQP);
    if (idx < 0) {
        UDSendQueue.emplace_back(std::move(buffer), AH, destQP);
    } else {
        UDOutstandingBuffers[idx] = std::move(buffer);
    }

    return 0;
}

int RDMAStack::trySendUDQPMessage(const temporary_buffer<uint8_t>& buffer, struct ibv_ah* AH, uint32_t destQP) {
    if (UDQPSRs.postedCount == SendWRData::maxWR) {
        return -1;
    }

    int idx = (UDQPSRs.postedIdx + UDQPSRs.postedCount) % SendWRData::maxWR;
    struct ibv_sge* SG = &(UDQPSRs.Segments[idx]);

    SG->addr = (uint64_t)buffer.get();
    SG->length = buffer.size();

    struct ibv_send_wr* SR = &(UDQPSRs.SendRequests[idx]);
    // We are relying that some of the SR parameters are initialized
    // once and not changed
    SR->next = nullptr;
    SR->sg_list = SG;

    SR->wr.ud.ah = AH;
    SR->wr.ud.remote_qpn = destQP;

    if (UDQPSRs.postedCount == SendWRData::signalThreshold) {
        SR->send_flags = IBV_SEND_SIGNALED;
    } else {
        SR->send_flags = 0;
    }

    struct ibv_send_wr* badSR;
    if (ibv_post_send(UDQP, SR, &badSR)) {
        std::cerr << "Failed to send" << std::endl;
        return -1;
    } else {
        UDQPSRs.postedCount++;
        return idx;
    }
}

bool RDMAStack::processUDSendQueue() {
    if (UDSendQueue.size() == 0 || UDQPSRs.postedCount == SendWRData::maxWR) {
        return false;
    }

    int toProcess = std::min((int)UDSendQueue.size(), (int)(SendWRData::maxWR - UDQPSRs.postedCount));
    int idx = (UDQPSRs.postedIdx + UDQPSRs.postedCount) % SendWRData::maxWR;
    int baseIdx = idx;
    struct ibv_send_wr* firstSR = &(UDQPSRs.SendRequests[idx]);

    for(int i=0; i < toProcess; ++i, idx = (baseIdx + i) % SendWRData::maxWR) {
        UDSend& sendData = UDSendQueue[i];
        struct ibv_sge* SG = &(UDQPSRs.Segments[idx]);
        struct ibv_send_wr* SR = &(UDQPSRs.SendRequests[idx]);

        SG->addr = (uint64_t)sendData.buffer.get();
        SG->length = sendData.buffer.size();
        SR->wr.ud.ah = sendData.AH;
        SR->wr.ud.remote_qpn = sendData.destQP;
        SR->sg_list = SG;

        if (i == toProcess - 1) {
            SR->next = nullptr;
            // TODO
            if (toProcess >= SendWRData::signalThreshold) {
                SR->send_flags = IBV_SEND_SIGNALED;
            }
        } else {
            SR->next = &(UDQPSRs.SendRequests[(baseIdx+i+1)%SendWRData::maxWR]);
            SR->send_flags = 0;
        }
    }

    struct ibv_send_wr* badSR;
    if (ibv_post_send(UDQP, firstSR, &badSR)) {
        std::cerr << "Failed to send" << std::endl;
        return false;
    }

    UDQPSRs.postedCount += toProcess;
    UDSendQueue.erase(UDSendQueue.cbegin(), UDSendQueue.cbegin()+toProcess);
    return true;
} 

void RDMAStack::freeUDSRs(uint64_t signaledID) {
    uint32_t freed=0;
    for (int i=UDQPSRs.postedIdx; i<=(int)signaledID; ++i, ++freed) {
        struct ibv_sge* SG = &(UDQPSRs.Segments[i]);
        // TODO temp buffers instead of free
        free((void*)SG->addr);
    }

    UDQPSRs.postedIdx = (UDQPSRs.postedIdx + freed) % SendWRData::maxWR;
    assert(freed <= UDQPSRs.postedCount);
    UDQPSRs.postedCount -= freed;
}


future<RDMAConnection> RDMAStack::accept() {
    if (acceptPromiseActive) {
        return make_exception_future<>();
    }

    if (acceptQueue.size()) {
        RDMAConnection conn = std::move(acceptQueue.back());
        acceptQueue.pop_back();
        return make_ready_future<>(std::move(conn));
    }

    acceptPromiseActive = true;
    acceptPromise = promise<>();
    return acceptPromise.get_future();
}

void RDMAStack::processUDMessage(UDMessage* message, EndPoint remote) {
    auto connIt = connectionLookup.find(remote);

    if (message->op == UDOps::HandshakeRequest) {
        if (connIt != connectionLookup.end()) {
            // TODO dedup
            return;
        }

        RDMAConnection conn(this, remote);
        conn.handshakeRequest(message->remoteQP);
        connectionLookup[remote] = conn.weak_from_this();
        if (acceptPromiseActive) {
            acceptPromiseActive = false;
            acceptPromise.set_value(std::move(conn));
        } else {
            acceptQueue.push_back(std::move(conn));
        }
    } else if (message->op == UDOps::HandshakeResponse) {
        if (connIt == connectionLookup.end() || !connIt->second) {
            // error, send response to remote?
            return;
        }

        connIt->second->handshakeResponse(message->remoteQP);
    } else {
        std::cerr << "Unknown UD op" << std::endl;
    }
}

bool RDMAStack::processUDCQ() {
    struct ibv_wc WCs[8];
    int completed = ibv_poll_cq(UDCQ, 8, WCs);
    assert(completed >= 0);

    if (completed == 0) {
        return false;
    }

    for (int i=0; i<completed; ++i) {
        if (WCs[i].opcode == IBV_WC_SEND) {
            freeUDSRs(WCs[i].wr_id);
            if (WCs[i].status != IBV_WC_SUCCESS) {
                std::cerr << "error on send wc" << std::endl;
            }
        } else {
            int idx = WCs[i].wr_id;

            if (WCs[i].status != IBV_WC_SUCCESS) {
                std::cerr << "error on UD recv wc" << std::endl;
            } else {
                UDMessage* message = (UDMessage*)(UDQPRRs.Segments[idx].data+40);
                struct ibv_grh* grh = UDQPRRs.Segments[idx].data;
                processUDMessage(message, EndPoint(grh->sgid, WCs[i].src_qp));
            }

            UDQPRRs.RecvRequests[idx].next = nullptr;
            struct ibv_recv_wr* badRR;
            if (ibv_post_recv(UDQP, &(UDQPRRs.RecvRequests[idx]), &badRR)) {
                std::cerr << "error on UD post_recv" << std::endl;
            }
        }
    }

    return true;
}

bool RDMAStack::poller() {
    bool didWork = processUDCQ();

    didWork |= processUDSendQueue();

    return didWork;
}

void RDMAStack::registerPoller() {
    RDMAPoller = reactor::poller::simple([&] { return poller(); });
}

std::unique_ptr<RDMAStack> RDMAStack::makeUDQP(std::unique_ptr<RDMAStack> stack) {
    // Step 1. Create a CQ
    stack->UDCQ = ibv_create_cq(ctx, RecvWRData::maxWR+SendWRData::maxWR, nullptr, nullptr, 0);
    if (!stack->UDCQ) { 
        std::cerr << "failed to create UDCQ" << std::endl;
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    // Step 2. Create the QP
    struct ibv_qp_init_attr initAttr = {
        .qp_context = nullptr,
        .send_cq = stack->UDCQ,
        .recv_cq = stack->UDCQ,
        .cap = {
            .max_send_wr = SendWRData::maxWR,
            .max_recv_wr = RecvWRData::maxWR,
            .max_send_sge = 1,
            .max_recv_sge = 1,
            .max_inline_data = 0}, // TODO optimizations for inline data
        .qp_type = IBV_QPT_UD,
        .sq_sig_all = 0
    };
    stack->UDQP = ibv_create_qp(stack->protectionDomain, &initAttr);
    if (!stack->UDQP) {
        std::cout << "failed to create UDQP" << std::endl;
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    // Step 3. Transition QP into INIT state
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = 1;
    if (ibv_modify_qp(stack->UDQP, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY)) {
        std::cerr << "failed to transition into init state" << std::endl;
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    // Step 4. Post receive requests
    for (int i=0; i<RecvWRData::maxWR; ++i) {
        struct ibv_recv_wr& RR = stack->UDQPRRs.RecvRequests[i];
        struct ibv_sge& SG = stack->UDQPRRs.Segments[i];

        RR.wr_id = i;
        if (i == RecvWRData::maxWR-1) {
            RR.next = nullptr;
        } else {
            RR.next = &(stack->UDQPRRs.RecvRequests[i+1]);
        }
        SG.addr = (uint64_t)malloc(UDQPRxSize);
        SG.length = UDQPRxSize;
        SG.lkey = stack->memRegionHandle->lkey;
        RR.sg_list = &SG;
        RR.num_sge = 1;
    } 
    struct ibv_recv_wr* badRR;
    if (ibv_post_recv(stack->UDQP, stack->UDQPRRs.RecvRequests, &badRR)) {
        std::cerr << "failed to post RRs" << std::endl;
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    // Step 5. Transition QP into ready-to-receive (RTR) state
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTR;
    if (ibv_modify_qp(stack->UDQP, &attr, IBV_QP_STATE)) {
        std::cerr << "failed to transition into RTR" << std::endl;
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    // Step 6. Transition QP into ready-to-send (RTS) state
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 0;
    if (ibv_modify_qp(stack->UDQP, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN)) {
        std::cerr << "failed to transition into RTS" << std::endl;
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    // Step 7. Prep Send WRs
    for (int i=0; i<SendWRData::maxWR; ++i) {
        struct ibv_send_wr& SR = stack->UDQPSRs.SendRequests[i];
        memset(&SR, 0, sizeof(struct ibv_send_wr));
        SR.wr_id = i;
        SR.num_sge = 1;
        SR.opcode = IBV_WR_SEND;
        SR.wr.ud.remote_qkey = 0;
    }

    std::cout << "Control QP num: " << stack->UDQP->qp_num << std::endl;

    return stack;
}

std::unique_ptr<RDMAStack> RDMAStack::makeRDMAStack(void* memRegion, size_t memRegionSize) {
    assert(initialized);

    std::unique_ptr<RDMAStack> stack = std::make_unique<RDMAStack>();

    stack->protectionDomain = ibv_alloc_pd(ctx);
    if (!stack->protectionDomain) {
        std::cerr << "ibv_alloc_pd failed" << std::endl;
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    stack->memRegionHandle = ibv_reg_mr(stack->protectionDomain, memRegion, memRegionSize, IBV_ACCESS_LOCAL_WRITE);
    if (!stack->memRegionHandle) {
        std::cerr << "failed to register memory" << std::endl;
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    //Port 1 index 1 should be our ROCEv2 gid
    ibv_query_gid(ctx, 1, 1, &(stack->myGID));

    stack = makeUDQP(std::move(stack));
    if (!stack) {
        return stack;
    }

    stack->registerPoller();
    //TODO expose our GID and UDQP
    
    return stack;
}



} // namespace rdma
} // namespace seastar

