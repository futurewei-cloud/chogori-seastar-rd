#include <iostream>

#include <seastar/net/rdma.hh>

#include <infiniband/verbs.h>


namespace seastar {

namespace rdma {

static bool initialized = false;
static struct ibv_context* ctx = nullptr;

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

int sendUDQPMessage(char* data, int size, struct ibv_ah* AH, uint32_t destQP) {
    if (size + 40 > UDQPRxSize) {
        std::cerr << "Message too large, max size is: " << UDQPRxSize-40 << std::endl;
        assert(false);
        return -1;
    }

    if (trySendUDQPMessage(data, size, AH, destQP)) {
        UDQPSendQueue.emplace_back(data, size);
    }

    return 0;
}

int RDMAStack::trySendUDQPMessage(char* data, int size, struct ibv_ah* AH, uint32_t destQP) {
    if (UDQPSRs.postedCount == SRData::maxWR) {
        return -1;
    }

    int idx = (UDQPSRs.postedIdx + UDQPSRs.postedCount) % SRData::maxWR;
    struct ibv_sge* SG = &(UDQPSRs.Segments[idx]);

    SG->addr = (uint64_t)data;
    SG->length = size;

    struct ibv_send_wr* SR = &(UDQPSRs.SendRequests[idx]);
    // We are relying that some of the SR parameters are initialized
    // once and not changed
    SR.next = nullptr;
    SR.sg_list = SG;

    // AH creation takes ~100usec, needs to be on slow core
    SR.wr.ud.ah = AH;
    SR.wr.ud.remote_qpn = destQP;

    if (UDQPSRs.postedCount == SRData::signalThershold) {
        SR.send_flags = IBV_SEND_SIGNALED;
    } else {
        SR.send_flags = 0;
    }

    struct ibv_send_wr* badSR;
    if (ibv_post_send(controlQP, SR, &badSR)) {
        std::cerr << "Failed to send" << std::endl;
        return -1;
    } else {
        UDQPSRs.postedCount++;
        return 0;
    }
}

bool RDMAStack::processUDSendQueue() {
    if (UDSendQueue.size() == 0 || UDQPSRs.postedCount == SRData::maxWR) {
        return false;
    }

    int toProcess = std::min(UDSendQueue.size(), SRData::maxWR - UDQPSRs.postedCount);
    int idx = (UDQPSRs.postedIdx + UDQPSRs.postedCount) % SRData::maxWR;
    int baseIdx = idx;
    struct ibv_send_wr* firstSR = &(UDQPSRs.SendRequests[idx]);

    for(int i=0; i < toProcess; ++i, idx = (baseIdx + i) % SRData::maxWR) {
        UDSend& sendData = UDSendQueue[i];
        struct ibv_sge* SG = &(UDQPSRs.Segments[idx]);
        struct ibv_send_wr* SR = &(UDQPSRs.SendRequests[idx]);

        SG->addr = (uint64_t)sendData.data;
        SG->length = sendData.size;
        SR.wr.ud.ah = sendData.AH;
        SR.wr.ud.remote_qpn = sendData.destQP;
        SR.sg_list = SG;

        if (i == toProcess - 1) {
            SR.next = nullptr;
            if (toProcess >= SRData::signalThreshold) {
                SR.send_flags = IBV_SEND_SIGNALED;
            }
        } else {
            SR.next = &(UDQPSRs.SendRequests[(baseIdx+i+1)%SRData::maxWR]);
            SR.send_flags = 0;
        }
    }

    struct ibv_send_wr* badSR;
    if (ibv_post_send(controlQP, firstSR, &badSR)) {
        std::cerr << "Failed to send" << std::endl;
        return false;
    }

    UDQPSRs.postedCount += toProcess;
    UDSendQueue.erase(UDSendQueue.cbegin(), UDSendQueue.cbegin()+toProcess);
    return true;
}

void RDMAStack::freeUDSRs(uint64_t signaledID) {
    int freed=0;
    for (int i=UDQPSRs.postedIdx; i<=signaledID; ++i, ++freed) {
        struct ibv_sge* SG = &(UDQPSRs.Segments[i]);
        free((void*)SG->addr);
    }

    UDQPSRs.postedIdx = (UDQPSRs.postedIdx + freed) % SRData::maxWR;
    assert(freed <= UDQPSRs.postedCount);
    UDQPSRs.postedCount -= freed;
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
            freeSRs(WCs[i].wr_id);
            if (WCs[i].status != IBV_WC_SUCCESS) {
                std::cerr << "error on send wc" << std::endl;
            }
        } else {
            // TODO process rx
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
    poller = reactor::poller::simple([&] { return poller(); });
}

static std::unique_ptr<RDMAStack> RDMAStack::makeControlQP(std::unique_ptr<RDMAStack> stack) {
    // Step 1. Create a CQ
    stack->UDCQ = ibv_create_cq(ctx, WRData::maxWR*2, nullptr, nullptr, 0);
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
            .max_send_wr = WRData::maxWR,
            .max_recv_wr = WRData::maxWR,
            .max_send_sge = 1,
            .max_recv_sge = 1,
            .max_inline_data = 0}, // TODO optimizations for inline data
        .qp_type = IBV_QPT_UD,
        .sq_sig_all = 0
    };
    stack->UDQP = ibv_create_qp(stack->pd, &initAttr);
    if (!stack->UDQP) {
        std::cout << "failed to create UDQP" << std::endl;
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    // Step 3. Transition QP into INIT state
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = 1;
    if (ibv_modify_qp(stack->controlQP, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY)) {
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
        SG.lkey = stack->mr->lkey;
        RR.sg_list = &SG;
        RR.num_sge = 1;
    } 
    struct ibv_recv_wr* badRR;
    if (ibv_post_recv(stack->UDQP, UDQPRRs.RecvRequests, &badRR)) {
        std::cerr << "failed to post RRs" << std::endl;
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    // Step 5. Transition QP into ready-to-receive (RTR) state
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTR;
    if (ibv_modify_qp(driver->controlQP, &attr, IBV_QP_STATE)) {
        std::cerr << "failed to transition into RTR" << std::endl;
        return std::unique_ptr<RDMADriver>(nullptr);
    }

    // Step 6. Transition QP into ready-to-send (RTS) state
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 0;
    if (ibv_modify_qp(driver->controlQP, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN)) {
        std::cerr << "failed to transition into RTS" << std::endl;
        return std::unique_ptr<RDMADriver>(nullptr);
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

    std::cout << "Control QP num: " << driver->controlQP->qp_num << std::endl;

    return driver;
}

static std::unique_ptr<RDMAStack> RDMAStack::makeRDMAStack(void* memRegion, size_t memRegionSize) {
    assert(initialized);

    std::unique_ptr<RDMAStack> stack = std::make_unique<RDMAStack>();

    stack->pd = ibv_alloc_pd(ctx);
    if (!stack->pd) {
        std::cerr << "ibv_alloc_pd failed" << std::endl;
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    stack->mr = ibv_reg_mr(stack->pd, memRegion, memRegionSize, IBV_ACCESS_LOCAL_WRITE);
    if (!stack->mr) {
        std::cerr << "failed to register memory" << std::endl;
        return std::unique_ptr<RDMADriver>(nullptr);
    }

    //Port 1 index 1 should be our ROCEv2 gid
    ibv_query_gid(ctx, 1, 1, &(stack->myGID));

    stack = makeUDQP(std::move(stack));
    if (!stack) {
        return stack;
    }

    driver->registerPoller();
    //TODO expose our GID and UDQP
    
    return stack;
}



} // namespace rdma
} // namespace seastar

