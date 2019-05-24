#include <iostream>
#include <algorithm>
#include <iterator>

#include "Log.h"
#include <seastar/net/rdma.hh>

#include <arpa/inet.h>
#include <infiniband/verbs.h>

bool operator==(const union ibv_gid& lhs, const union ibv_gid& rhs) {
    return (memcmp(lhs.raw, rhs.raw, 16) == 0);
}
// TODO investigate smarter hash algorithms
std::size_t std::hash<seastar::rdma::EndPoint>::operator()(const seastar::rdma::EndPoint& endpoint) const {
    return std::hash<union ibv_gid>{}(endpoint.GID) ^ std::hash<uint32_t>{}(endpoint.UDQP);
}

namespace seastar {

namespace rdma {

static bool initialized = false;
// TODO ref count RDMAStacks and free context?
static struct ibv_context* ctx = nullptr;

void RDMAConnection::makeQP() {
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
        K2ERROR("Failed to create RC QP");
        errorState = true;
        return;
    }

    // Step 3. Transition QP to INIT state
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = 1;
    if (ibv_modify_qp(QP, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
        K2ERROR("failed to transition RC QP into init state")
        errorState = true;
        ibv_destroy_qp(QP);
        QP = nullptr;
        return;
    }
    // end of slow core part

    stack->RCConnectionCount++;
    if (stack->RCConnectionCount > RDMAStack::maxExpectedConnections) {
        K2WARN("CQ overrun possible");
    }
    stack->RCLookup[QP->qp_num] = weak_from_this();
}

void RDMAConnection::makeHandshakeRequest() {
    makeQP();
    if (!errorState) {
        uint32_t id = stack->sendHandshakeRequest(remote, QP->qp_num);
        stack->handshakeLookup[id] = weak_from_this();
    }
}

void RDMAConnection::processHandshakeRequest(uint32_t remoteQP, uint32_t responseId) {
    makeQP();
    if (!errorState) {
        stack->sendHandshakeResponse(remote, QP->qp_num, responseId);
        completeHandshake(remoteQP);
    }
}

void RDMAStack::fillAHAttr(struct ibv_ah_attr& AHAttr, const union ibv_gid& GID) {
    memset(&AHAttr, 0, sizeof(struct ibv_ah_attr));
    memcpy(AHAttr.grh.dgid.raw, GID.raw, 16);
    AHAttr.grh.sgid_index = 1; // Index 1 is ROCEv2 address
    AHAttr.grh.hop_limit = 64; // Equivalent to IPv4 time to live
    AHAttr.is_global = 1; // Means use GID
    // For ConnectX-4 EN, we have one port per RDMA device, which starts at "1"
    AHAttr.port_num = 1;
}

void RDMAConnection::completeHandshake(uint32_t remoteQP) {
    // TODO move this part to slow core
    struct ibv_ah_attr AHAttr;
    RDMAStack::fillAHAttr(AHAttr, remote.GID);

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
        K2ERROR("Failed to transition RC QP into RTR");
        errorState = true;
        ibv_destroy_qp(QP);
        QP = nullptr;
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
        K2ERROR("Failed to transition into RTS");
        errorState = true;
        ibv_destroy_qp(QP);
        QP = nullptr;
        return;
    }
    // End of slow core part

    isReady = true;
    processSends<std::deque<temporary_buffer<uint8_t>>>(sendQueue);
}

template <class VecType>
bool RDMAConnection::processSends(VecType& queue) {
    if (queue.size() == 0 || sendWRs.postedCount == SendWRData::maxWR) {
        return false;
    }

    int toProcess = std::min((int)queue.size(), (int)(SendWRData::maxWR - sendWRs.postedCount));
    int idx = (sendWRs.postedIdx + sendWRs.postedCount) % SendWRData::maxWR;
    int baseIdx = idx;
    struct ibv_send_wr* firstSR = &(sendWRs.SendRequests[idx]);

    for(int i=0; i < toProcess; ++i, idx = (baseIdx + i) % SendWRData::maxWR) {
        temporary_buffer<uint8_t>& sendData = queue[i];
        struct ibv_sge* SG = &(sendWRs.Segments[idx]);
        struct ibv_send_wr* SR = &(sendWRs.SendRequests[idx]);

        SG->addr = (uint64_t)sendData.get();
        SG->length = sendData.size();
        SR->sg_list = SG;

        if ((idx+1) % SendWRData::signalThreshold == 0) {
            SR->send_flags = IBV_SEND_SIGNALED;
        } else {
            SR->send_flags = 0;
        }

        if (i == toProcess - 1) {
            SR->next = nullptr;
        } else {
            SR->next = &(sendWRs.SendRequests[(baseIdx+i+1)%SendWRData::maxWR]);
        }

        outstandingBuffers[idx] = std::move(sendData);
    }

    struct ibv_send_wr* badSR;
    if (ibv_post_send(QP, firstSR, &badSR)) {
        K2ASSERT(false, "Failed to post send on RC QP");
        return false;
    }

    sendWRs.postedCount += toProcess;
    queue.erase(queue.cbegin(), queue.cbegin()+toProcess);
    return true;
}

future<temporary_buffer<uint8_t>> RDMAConnection::recv() {
    // TODO check for connection error state and return exception if needed
    if (errorState) {
        return make_exception_future<temporary_buffer<uint8_t>>(RDMAConnectionError());
    }
    if (recvPromiseActive) {
        K2ASSERT(false, "recv() called with promise already active");
        throw RDMAConnectionError();
    }

    if (recvQueue.size()) {
        temporary_buffer<uint8_t> buf = std::move(recvQueue.front());
        recvQueue.pop_front();
        return make_ready_future<temporary_buffer<uint8_t>>(std::move(buf));
    }

    recvPromise = promise<temporary_buffer<uint8_t>>();
    recvPromiseActive = true;
    return recvPromise.get_future();
}

void RDMAConnection::incomingMessage(unsigned char* data, uint32_t size) {
    temporary_buffer<uint8_t> buf(data, size, make_free_deleter(data));
    if (recvPromiseActive) {
        recvPromiseActive = false;
        recvPromise.set_value(std::move(buf));
    } else {
        recvQueue.push_back(std::move(buf));
    }
}

void RDMAConnection::send(std::vector<temporary_buffer<uint8_t>>&& buf) {
    if (!isReady || sendQueue.size()) {
        sendQueue.insert(sendQueue.end(), std::make_move_iterator(buf.begin()),
                         std::make_move_iterator(buf.end()));
        return;
    }

    processSends<std::vector<temporary_buffer<uint8_t>>>(buf);
    if (buf.size()) {
        sendQueue.insert(sendQueue.end(), std::make_move_iterator(buf.begin()),
                         std::make_move_iterator(buf.end()));
    }
}

RDMAConnection::~RDMAConnection() noexcept {
    // TODO slow core? reset QP to init state?
    if (QP) {
        ibv_destroy_qp(QP);
        QP = nullptr;
        stack->RCConnectionCount--;
    }
}

/*
RDMAConnection::RDMAConnection(RDMAConnection&& conn) noexcept {
    isReady = conn.isReady;
    errorState = conn.errorState;
    recvQueue = std::move(conn.recvQueue);
    sendQueue = std::move(conn.sendQueue);
    sendWRs = std::move(conn.sendWRs);
    outstandingBuffers = std::move(conn.outstandingBuffers);
    QP = conn.QP;
    conn.QP = nullptr;
    stack = conn.stack;
    conn.stack = nullptr;
    remote = std::move(conn.remote);
    recvPromiseActive = conn.recvPromiseActive;
    recvPromise = std::move(conn.recvPromise);
}

RDMAConnection& RDMAConnection::operator=(RDMAConnection&& conn) noexcept {
    if (this == &conn) {
        return *this;
    }

    isReady = conn.isReady;
    errorState = conn.errorState;
    recvQueue = std::move(conn.recvQueue);
    sendQueue = std::move(conn.sendQueue);
    sendWRs = std::move(conn.sendWRs);
    outstandingBuffers = std::move(conn.outstandingBuffers);
    QP = conn.QP;
    conn.QP = nullptr;
    stack = conn.stack;
    conn.stack = nullptr;
    remote = conn.remote;
    recvPromiseActive = conn.recvPromiseActive;
    recvPromise = std::move(conn.recvPromise);

    return *this;
}
*/

int initRDMAContext() {
    if (initialized) {
        return 0;
    }

    int numDevices;
    struct ibv_device** devices = ibv_get_device_list(&numDevices);
    if (!devices) {
        K2ERROR("ibv_get_device_list failed");
        return -1;
    }

    // TODO get correct device from configuration
    int deviceIdx=0;
    for(; deviceIdx < numDevices; ++deviceIdx) {
        std::cerr << ibv_get_device_name(devices[deviceIdx]) << std::endl;
        if (strcmp(ibv_get_device_name(devices[deviceIdx]), "mlx5_1") == 0) {
            break;
        }
    }

    if (deviceIdx == numDevices) {
        ibv_free_device_list(devices);
        K2WARN("No suitable RDMA device found");
        return -1;
    }

    ctx = ibv_open_device(devices[deviceIdx]);
    K2ASSERT(ctx, "ibv_open_device failed");

    ibv_free_device_list(devices);

    initialized = true;
    return 0;
}

RDMAListener RDMAStack::listen() {
    return RDMAListener(this);
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

void RDMAStack::sendHandshakeResponse(const EndPoint& endpoint, uint32_t QPNum, uint32_t id) {
    temporary_buffer<uint8_t> response(sizeof(UDMessage));
    UDMessage* message = (UDMessage*)response.get();
    message->op = UDOps::HandshakeResponse;
    message->QPNum = QPNum;
    message->requestId = id;
    sendUDQPMessage(std::move(response), endpoint.GID, endpoint.UDQP);
}

uint32_t RDMAStack::sendHandshakeRequest(const EndPoint& endpoint, uint32_t QPNum) {
    temporary_buffer<uint8_t> response(sizeof(UDMessage));
    UDMessage* message = (UDMessage*)response.get();
    message->op = UDOps::HandshakeRequest;
    message->QPNum = QPNum;
    uint32_t id = handshakeId++;
    message->requestId = id;
    sendUDQPMessage(std::move(response), endpoint.GID, endpoint.UDQP);
    return id;
}

struct ibv_ah* RDMAStack::makeAH(const union ibv_gid& GID) {
    struct ibv_ah_attr AHAttr;
    fillAHAttr(AHAttr, GID);

    struct ibv_ah* AH = ibv_create_ah(protectionDomain, &AHAttr);
    K2ASSERT(AH, "Failed to create AH");

    AHLookup[GID] = AH;
    return AH;
}

int RDMAStack::sendUDQPMessage(temporary_buffer<uint8_t> buffer, const union ibv_gid& destGID, uint32_t destQP) {
    K2ASSERT(buffer.size() + 40 <= UDQPRxSize, "UD Message too large");

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

    if ((idx+1) % SendWRData::signalThreshold == 0) {
        SR->send_flags = IBV_SEND_SIGNALED;
    } else {
        SR->send_flags = 0;
    }

    struct ibv_send_wr* badSR;
    if (ibv_post_send(UDQP, SR, &badSR)) {
        K2ASSERT(false, "Failed to post UD send");
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

        if ((idx+1) % SendWRData::signalThreshold == 0) {
            SR->send_flags = IBV_SEND_SIGNALED;
        } else {
            SR->send_flags = 0;
        }

        if (i == toProcess - 1) {
            SR->next = nullptr;
        } else {
            SR->next = &(UDQPSRs.SendRequests[(baseIdx+i+1)%SendWRData::maxWR]);
        }

        UDOutstandingBuffers[idx] = std::move(sendData.buffer);
    }

    struct ibv_send_wr* badSR;
    if (ibv_post_send(UDQP, firstSR, &badSR)) {
        K2ASSERT(false, "Failed to post UD send");
        return false;
    }

    UDQPSRs.postedCount += toProcess;
    UDSendQueue.erase(UDSendQueue.cbegin(), UDSendQueue.cbegin()+toProcess);
    return true;
}

void RDMAStack::processCompletedSRs(std::array<temporary_buffer<uint8_t>, SendWRData::maxWR>& buffers, SendWRData& WRData, uint64_t signaledID) {
    uint32_t freed=0;
    for (int i=WRData.postedIdx; i<=(int)signaledID; ++i, ++freed) {
        buffers[i] = temporary_buffer<uint8_t>();
    }

    WRData.postedIdx = (WRData.postedIdx + freed) % SendWRData::maxWR;
    K2ASSERT(freed <= WRData.postedCount, "Bug in circular buffer for SRs");
    WRData.postedCount -= freed;
}

future<std::unique_ptr<RDMAConnection>> RDMAStack::accept() {
    if (acceptPromiseActive) {
        throw RDMAConnectionError();
    }

    if (acceptQueue.size()) {
        std::unique_ptr<RDMAConnection> conn = std::move(acceptQueue.back());
        acceptQueue.pop_back();
        return make_ready_future<std::unique_ptr<RDMAConnection>>(std::move(conn));
    }

    acceptPromiseActive = true;
    acceptPromise = promise<std::unique_ptr<RDMAConnection>>();
    return acceptPromise.get_future();
}

std::unique_ptr<RDMAConnection> RDMAStack::connect(const EndPoint& remote) {
    std::unique_ptr<RDMAConnection> conn = std::make_unique<RDMAConnection>(this, remote);
    conn->makeHandshakeRequest();
    return conn;
}

void RDMAStack::processUDMessage(UDMessage* message, EndPoint remote) {
    if (message->op == UDOps::HandshakeRequest) {
        std::unique_ptr<RDMAConnection> conn = std::make_unique<RDMAConnection>(this, remote);
        handshakeLookup[message->requestId] = conn->weak_from_this();
        conn->processHandshakeRequest(message->QPNum, message->requestId);

        if (acceptPromiseActive) {
            acceptPromiseActive = false;
            acceptPromise.set_value(std::move(conn));
        } else {
            acceptQueue.push_back(std::move(conn));
        }
    } else if (message->op == UDOps::HandshakeResponse) {
        auto connIt = handshakeLookup.find(message->requestId);
        if (connIt == handshakeLookup.end()) {
            K2WARN("Unsolicited Handshake response");
            return;
        }
        else if (!connIt->second) {
            K2WARN("Connection deleted before handshake completed");
            handshakeLookup.erase(connIt);
            return;
        }

        connIt->second->completeHandshake(message->QPNum);
        handshakeLookup.erase(connIt);
    } else {
        K2ASSERT(false, "Unknown UD op");
    }
}

bool RDMAStack::processRCCQ() {
    struct ibv_wc WCs[8];
    int completed = ibv_poll_cq(UDCQ, 8, WCs);
    K2ASSERT(completed >= 0, "Failed to poll RC CQ");

    if (completed == 0) {
        return false;
    }

    int recvWCs = 0;
    int firstRecvIdx = 0;

    for (int i=0; i<completed; ++i) {
        bool foundConn = true;
        auto connIt = RCLookup.find(WCs[i].qp_num);
        if (connIt == RCLookup.end()) {
            foundConn = false;
        }
        if (foundConn && !connIt->second) {
            RCLookup.erase(connIt);
            foundConn = false;
        }

        // TODO opcodes are not valid if there is an error, fix handling
        if (WCs[i].opcode == IBV_WC_SEND) {
            // We can ignore send completions for connections that no longer exist
            if (!foundConn) {
                continue;
            }

            weak_ptr<RDMAConnection>& conn = RCLookup[WCs[i].qp_num];
            processCompletedSRs(conn->outstandingBuffers, conn->sendWRs, WCs[i].wr_id);
            if (WCs[i].status != IBV_WC_SUCCESS) {
                K2WARN("error on send wc");
                conn->errorState = true;
            }
            conn->processSends(conn->sendQueue);
        } else {
            int idx = WCs[i].wr_id;
            if (recvWCs == 0) {
                firstRecvIdx = idx;
            }
            ++recvWCs;

            if (WCs[i].status != IBV_WC_SUCCESS) {
                K2WARN("error on recv wc");
                if (foundConn) {
                    weak_ptr<RDMAConnection>& conn = RCLookup[WCs[i].qp_num];
                    conn->errorState = true;
                }
            } else if (foundConn) {
                unsigned char* data = (unsigned char*)RCQPRRs.Segments[idx].addr;
                uint32_t size = WCs[i].byte_len;
                weak_ptr<RDMAConnection>& conn = RCLookup[WCs[i].qp_num];
                conn->incomingMessage(data, size);
            }
        }
    }

    for (int i=0; i<recvWCs; ++i) {
        int idx = (firstRecvIdx + i) % RecvWRData::maxWR;
        struct ibv_recv_wr& RR = RCQPRRs.RecvRequests[idx];

        if (i == recvWCs-1) {
            RR.next = nullptr;
        } else {
            RR.next = &(RCQPRRs.RecvRequests[(idx+1)%RecvWRData::maxWR]);
        }
        RCQPRRs.Segments[idx].addr = (uint64_t)malloc(RCDataSize);
    }

    struct ibv_recv_wr* badRR;
    if (ibv_post_srq_recv(SRQ, &(RCQPRRs.RecvRequests[firstRecvIdx]), &badRR)) {
        K2ASSERT(false, "error on RC post_recv");
    }

    return true;

}

bool RDMAStack::processUDCQ() {
    struct ibv_wc WCs[8];
    int completed = ibv_poll_cq(UDCQ, 8, WCs);
    K2ASSERT(completed >= 0, "Failed to poll UD CQ");

    if (completed == 0) {
        return false;
    }

    // TODO opcodes are not valid if there is an error, fix handling
    for (int i=0; i<completed; ++i) {
        if (WCs[i].opcode == IBV_WC_SEND) {
            processCompletedSRs(UDOutstandingBuffers, UDQPSRs, WCs[i].wr_id);
            if (WCs[i].status != IBV_WC_SUCCESS) {
                K2WARN("error on send wc");
            }
        } else {
            int idx = WCs[i].wr_id;

            if (WCs[i].status != IBV_WC_SUCCESS) {
                K2WARN("error on UD recv wc");
                K2WARN(ibv_wc_status_str(WCs[i].status));
            } else {
                UDMessage* message = (UDMessage*)(UDQPRRs.Segments[idx].addr+40);
                struct ibv_grh* grh = (struct ibv_grh*)UDQPRRs.Segments[idx].addr;
                processUDMessage(message, EndPoint(grh->sgid, WCs[i].src_qp));
            }

            UDQPRRs.RecvRequests[idx].next = nullptr;
            struct ibv_recv_wr* badRR;
            if (ibv_post_recv(UDQP, &(UDQPRRs.RecvRequests[idx]), &badRR)) {
                K2ASSERT(false, "error on UD post_recv");
            }
        }
    }

    return true;
}

bool RDMAStack::poller() {
    bool didWork = processUDCQ();

    didWork |= processUDSendQueue();
    didWork |= processRCCQ();

    return didWork;
}

void RDMAStack::registerPoller() {
    RDMAPoller = reactor::poller::simple([&] { return poller(); });
}

std::unique_ptr<RDMAStack> RDMAStack::makeUDQP(std::unique_ptr<RDMAStack> stack) {
    // Step 1. Create a CQ
    stack->UDCQ = ibv_create_cq(ctx, RecvWRData::maxWR+SendWRData::maxWR, nullptr, nullptr, 0);
    if (!stack->UDCQ) {
        K2ERROR("failed to create UDCQ");
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
        K2ERROR("Failed to create UDQP");
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    // Step 3. Transition QP into INIT state
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = 1;
    if (ibv_modify_qp(stack->UDQP, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY)) {
        K2ERROR("Failed to transition UD QP into init state");
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
        K2ERROR("Failed to post UD QP RRs");
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    // Step 5. Transition QP into ready-to-receive (RTR) state
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTR;
    if (ibv_modify_qp(stack->UDQP, &attr, IBV_QP_STATE)) {
        K2ERROR("Failed to transition UD QP into RTR");
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    // Step 6. Transition QP into ready-to-send (RTS) state
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 0;
    if (ibv_modify_qp(stack->UDQP, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN)) {
        K2ERROR("Failed to transition UD QP into RTS");
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

        stack->UDQPSRs.Segments[i].lkey = stack->memRegionHandle->lkey;
    }

    stack->localEndpoint.UDQP = stack->UDQP->qp_num;

    return stack;
}

std::unique_ptr<RDMAStack> RDMAStack::makeRDMAStack(void* memRegion, size_t memRegionSize) {
    if (!initialized || !ctx) {
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    std::unique_ptr<RDMAStack> stack = std::make_unique<RDMAStack>();

    stack->protectionDomain = ibv_alloc_pd(ctx);
    if (!stack->protectionDomain) {
        K2ERROR("ibv_alloc_pd failed");
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    stack->memRegionHandle = ibv_reg_mr(stack->protectionDomain, memRegion, memRegionSize, IBV_ACCESS_LOCAL_WRITE);
    if (!stack->memRegionHandle) {
        K2ERROR("Failed to register memory");
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    //Port 1 index 1 should be our ROCEv2 gid
    ibv_query_gid(ctx, 1, 1, &(stack->localEndpoint.GID));

    stack = makeUDQP(std::move(stack));
    if (!stack) {
        return stack;
    }

    stack->RCCQ = ibv_create_cq(ctx, RCCQSize, nullptr, nullptr, 0);
    if (!stack->RCCQ) {
        K2ERROR("Failed to create RCCQ");
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    struct ibv_srq_init_attr SRQAttr = {
        .srq_context = nullptr,
        .attr = {
            .max_wr = RecvWRData::maxWR,
            .max_sge = 1,
            .srq_limit = 0}
    };
    stack->SRQ = ibv_create_srq(stack->protectionDomain, &SRQAttr);
    if (!stack->SRQ) {
        K2ERROR("Failed to create SRQ");
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    for (int i=0; i<RecvWRData::maxWR; ++i) {
        struct ibv_recv_wr& RR = stack->RCQPRRs.RecvRequests[i];
        struct ibv_sge& SG = stack->RCQPRRs.Segments[i];

        RR.wr_id = i;
        if (i == RecvWRData::maxWR-1) {
            RR.next = nullptr;
        } else {
            RR.next = &(stack->RCQPRRs.RecvRequests[i+1]);
        }
        SG.addr = (uint64_t)malloc(RCDataSize);
        SG.length = RCDataSize;
        SG.lkey = stack->memRegionHandle->lkey;
        RR.sg_list = &SG;
        RR.num_sge = 1;
    }
    struct ibv_recv_wr* badRR;
    if (ibv_post_srq_recv(stack->SRQ, stack->RCQPRRs.RecvRequests, &badRR)) {
        K2ERROR("failed to post SRQ RRs");
        return std::unique_ptr<RDMAStack>(nullptr);
    }

    stack->registerPoller();

    return stack;
}

sstring GIDToString(union ibv_gid gid) {
    char buffer[INET6_ADDRSTRLEN];
    buffer[0] = '\0';
    static_assert(sizeof(struct in6_addr) == sizeof(union ibv_gid));
    assert(sizeof(struct in6_addr) == sizeof(union ibv_gid));
    // parse the gid as an ipv6
    ::inet_ntop(AF_INET6, &gid.raw, buffer, INET6_ADDRSTRLEN);
    return sstring(buffer);
}

int StringToGID(const sstring& strGID, union ibv_gid& result) {
    static_assert(sizeof(struct in6_addr) == sizeof(union ibv_gid));
    // use ipv6 parser
    if (::inet_pton(AF_INET6, strGID.c_str(), &result.raw) != 1) {
        return -1;
    }
    return 0;
}

} // namespace rdma
} // namespace seastar
