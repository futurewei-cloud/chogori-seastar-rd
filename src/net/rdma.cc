/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright Futurewei Technologies Inc, 2020
 */

#include <iostream>
#include <algorithm>
#include <iterator>
#include <memory>

#include "Log.h"
#include <seastar/net/rdma.hh>
#include <seastar/core/metrics_registration.hh> // metrics
#include <seastar/core/metrics.hh>
#include "core/thread_pool.hh"

#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <cstring>
#include <cerrno>

bool operator==(const union ibv_gid& lhs, const union ibv_gid& rhs) {
    return (memcmp(lhs.raw, rhs.raw, 16) == 0);
}
std::size_t std::hash<seastar::rdma::EndPoint>::operator()(const seastar::rdma::EndPoint& endpoint) const {
    return std::hash<union ibv_gid>{}(endpoint.GID) ^ std::hash<uint32_t>{}(endpoint.UDQP);
}

namespace seastar {

namespace rdma {

static bool initialized = false;
// TODO ref count RDMAStacks and free context?
static struct ibv_context* ctx = nullptr;

future<> RDMAConnection::makeQP() {
    // A Reliable Connected (RC) QP has five states:
    // RESET: the state of a newly created QP
    // INIT: Receive requests can be posted, but no receives or
    //       sends will occur
    // RTR: Ready-to-receive. Receives can be completed
    // RTS: Ready-to-send. Sends can be posted and completed
    // ERR: Error state.
    // To use a RC QP, the states must be transitioned in order:
    // RESET->INIT->RTR->RTS using ibv_modify_qp without skipping states
    // see the ibv_modify_qp documentation for more info

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

    return engine()._thread_pool->submit<struct ibv_qp*>([init_attr, PD=stack->protectionDomain]() mutable {
        // Create QP
        // For all ll ibv_* calls with attr (attribute) parameters, the attr is passed by pointer because
        // it is a C interface, but the struct does not need to live beyond the ibv_ call.
        struct ibv_qp* QP = ibv_create_qp(PD, &init_attr);
        if (!QP) {
            K2ERROR("Failed to create RC QP: " << strerror(errno));
            throw RDMAConnectionError();
        }

        // Transition QP to INIT state
        struct ibv_qp_attr attr = {};
        attr.qp_state = IBV_QPS_INIT;
        attr.port_num = 1;
        if (int err = ibv_modify_qp(QP, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                                               IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
            K2ERROR("failed to transition RC QP into init state: " << strerror(err));
            throw RDMAConnectionError();
        }

        return QP;
    }).
    then([conn=weak_from_this()] (struct ibv_qp* newQP) {
        if (!conn) {
            ibv_destroy_qp(newQP);
            return;
        }

        conn->QP = newQP;
        if (conn->stack->RCConnectionCount+1 > RDMAStack::maxExpectedConnections) {
            K2WARN("CQ overrun possible");
        }
        conn->stack->RCLookup[conn->QP->qp_num] = conn->weak_from_this();
    }).
    handle_exception([conn=weak_from_this()] (auto e) {
        if (!conn) {
            return;
        }

        conn->shutdownConnection();
    });
}

void RDMAConnection::makeHandshakeRequest() {
    (void) makeQP().then([conn=weak_from_this()] () {
        K2DEBUG("MakeQP done");
        if (!conn) {
            return;
        }

        if (!conn->errorState) {
            uint32_t id = conn->stack->sendHandshakeRequest(conn->remote, conn->QP->qp_num);
            conn->stack->handshakeLookup[id] = conn->weak_from_this();
            K2DEBUG("Handshake request sent");
        }
    });
}

void RDMAConnection::processHandshakeRequest(uint32_t remoteQP, uint32_t responseId) {
    (void) makeQP().then([conn=weak_from_this(), remoteQP, responseId] () {
        if (!conn) {
            return;
        }

        K2DEBUG("MakeQP done");
        if (!conn->errorState) {
            conn->stack->sendHandshakeResponse(conn->remote, conn->QP->qp_num, responseId);
            K2DEBUG("Handshake response sent");
            (void) conn->completeHandshake(remoteQP);
        }
    });
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

future<> RDMAConnection::completeHandshake(uint32_t remoteQP) {
    // A Reliable Connected (RC) QP has five states:
    // RESET: the state of a newly created QP
    // INIT: Receive requests can be posted, but no receives or
    //       sends will occur
    // RTR: Ready-to-receive. Receives can be completed
    // RTS: Ready-to-send. Sends can be posted and completed
    // ERR: Error state.
    // To use a RC QP, the states must be transitioned in order:
    // RESET->INIT->RTR->RTS using ibv_modify_qp without skipping states
    // see the ibv_modify_qp documentation for more info

    // This relies on the fact that QP deletion is also submitted to the
    // slow thread so that we know these modify_qp calls will be serialized
    // with deletion.
    return engine()._thread_pool->submit<int>([QP=this->QP, remote=this->remote, remoteQP]() {
        struct ibv_ah_attr AHAttr;
        RDMAStack::fillAHAttr(AHAttr, remote.GID);

        // The timeout, rnr_retry, and min_rnr_timer values of ibv_qp_attr
        // do not have specific units,
        // e.g. the value assigned to timeout is an index into a lookup table
        // of exponentially increasing values. See the ibv_modify_qp documentation
        // for more info
        // TODO implement an exponential backoff timeout/retry strategy in software

        // Transition QP to RTR state
        struct ibv_qp_attr attr = {};
        attr.qp_state = IBV_QPS_RTR;
        attr.ah_attr = AHAttr;
        attr.path_mtu = IBV_MTU_4096;
        attr.dest_qp_num = remoteQP;
        attr.rq_psn = 0;
        attr.max_dest_rd_atomic = 0;
        attr.min_rnr_timer = 1; // 0.01 millisecond
        if (int err = ibv_modify_qp(QP, &attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                                    IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                                    IBV_QP_MIN_RNR_TIMER | IBV_QP_DEST_QPN)) {
            K2ERROR("failed to transition RC QP into RTR: " << strerror(err));
            throw RDMAConnectionError();
        }

        // Transition QP to RTS state
        memset(&attr, 0, sizeof(ibv_qp_attr));
        attr.qp_state = IBV_QPS_RTS;
        attr.sq_psn = 0;
        attr.timeout = 8; // ~1ms
        attr.retry_cnt = 5;
        attr.rnr_retry = 7;
        attr.max_rd_atomic = 0;
        if (int err = ibv_modify_qp(QP, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN |
                                    IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                                    IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC)) {
            K2ERROR("failed to transition RC QP into RTS: " << strerror(err));
            throw RDMAConnectionError();
        }

        return 0;
    }).then([conn=weak_from_this()] (int) {
        if (!conn) {
            return;
        }

        // Prepare Send request data that does not change
        for (int i=0; i<SendWRData::maxWR; ++i) {
            struct ibv_send_wr& SR = conn->sendWRs.SendRequests[i];
            memset(&SR, 0, sizeof(struct ibv_send_wr));
            // We need to use the wr_id to differentiate between sends and receives
            // in the case of errors
            SR.wr_id = i + RecvWRData::maxWR;
            SR.num_sge = 1;
            SR.opcode = IBV_WR_SEND;

            conn->sendWRs.Segments[i].lkey = conn->stack->memRegionHandle->lkey;
        }

        K2DEBUG("Handshake complete");
        conn->stack->RCConnectionCount++;
        conn->isReady = true;

        size_t beforeSize = conn->sendQueue.size();
        conn->processSends<std::deque<Buffer>>(conn->sendQueue);
        conn->stack->sendQueueSize -= beforeSize - conn->sendQueue.size();
    }).handle_exception([conn=weak_from_this()] (auto e) {
        if (!conn) {
            return;
        }

        conn->shutdownConnection();
    });
}


// This method sends a zero-byte message to the destination, which
// indicates a graceful close. It also makes the send signaled so that
// we can know when the send queue is flushed
void RDMAConnection::sendCloseSignal() {
    if (!QP || errorState) {
        return;
    }

    int idx = (sendWRs.postedIdx + sendWRs.postedCount) % SendWRData::maxWR;
    struct ibv_send_wr* SR = &(sendWRs.SendRequests[idx]);

    SR->send_flags = IBV_SEND_SIGNALED;
    SR->next = nullptr;
    SR->sg_list = nullptr;
    SR->num_sge = 0;

    struct ibv_send_wr* badSR;
    if (int err = ibv_post_send(QP, SR, &badSR)) {
        K2ASSERT(false, "Failed to post send on RC QP: " << strerror(err));
        shutdownConnection();
        return;
    }

    // Even though this should be the last SR on this connection, we will
    // reset num_sge to the value the rest of the code expects
    SR->num_sge = 1;

    sendWRs.postedCount++;
}

template <class VecType>
void RDMAConnection::processSends(VecType& queue) {
    if (!QP) {
        // This case means an error occured and we shutdown the connection
        queue.clear();
        sendQueue.clear();
        sendWRs.postedCount = 0;
    }

    if (closePromiseActive && sendQueue.size() == 0 && sendWRs.postedCount == 0
                                                    && queue.size() == 0) {
        shutdownConnection();
        closePromiseActive = false;
        closePromise.set_value();
        return;
    }
    else if (queue.size() == 0 || sendWRs.postedCount == SendWRData::maxWR) {
        return;
    }

    int toProcess = std::min((int)queue.size(), (int)(SendWRData::maxWR - sendWRs.postedCount));
    int idx = (sendWRs.postedIdx + sendWRs.postedCount) % SendWRData::maxWR;
    int baseIdx = idx;
    struct ibv_send_wr* firstSR = &(sendWRs.SendRequests[idx]);

    for(int i=0; i < toProcess; ++i, idx = (baseIdx + i) % SendWRData::maxWR) {
        Buffer& sendData = queue[i];
        if (sendQueueTailBytesLeft && std::addressof(sendData) == std::addressof(sendQueue.back())) {
            sendData.trim(RDMAStack::RCDataSize - sendQueueTailBytesLeft);
            sendQueueTailBytesLeft = 0;
        }

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

    stack->totalSend += toProcess;
    stack->sendBatchSum += toProcess;
    stack->sendBatchCount++;

    struct ibv_send_wr* badSR;
    if (int err = ibv_post_send(QP, firstSR, &badSR)) {
        K2ASSERT(false, "Failed to post send on RC QP: " << strerror(err));
        shutdownConnection();
        return;
    }

    sendWRs.postedCount += toProcess;
    queue.erase(queue.cbegin(), queue.cbegin()+toProcess);
    return;
}

future<Buffer> RDMAConnection::recv() {
    if (errorState) {
        return make_ready_future<Buffer>(Buffer());
    }

    if (recvPromiseActive) {
        K2ASSERT(false, "recv() called with promise already active");
        return make_exception_future<Buffer>(std::runtime_error("recv() called with promise already active"));
    }

    if (recvQueue.size()) {
        auto recv_future = make_ready_future<Buffer>(std::move(recvQueue.front()));
        recvQueue.pop_front();
        return recv_future;
    }

    recvPromise = promise<Buffer>();
    recvPromiseActive = true;
    return recvPromise.get_future();
}

void RDMAConnection::incomingMessage(char* data, uint32_t size) {
    K2DEBUG("RDMAConn " << QP->qp_num << " got message of size: " << size);
    Buffer buf(data, size, make_free_deleter(data));
    if (recvPromiseActive) {
        recvPromiseActive = false;
        recvPromise.set_value(std::move(buf));
    } else {
        recvQueue.push_back(std::move(buf));
    }

    if (!size) {
        // This is a graceful close message
        shutdownConnection();
    }
}

void RDMAConnection::send(std::vector<Buffer>&& buf) {
    if (!isReady || sendQueue.size()) {
        size_t beforeSize = sendQueue.size();

        auto it = buf.begin();
        for (; it != buf.end(); ++it) {
            if (it->size() > messagePackThreshold) {
                if (sendQueueTailBytesLeft > 0) {
                    sendQueue.back().trim(RDMAStack::RCDataSize-sendQueueTailBytesLeft);
                    sendQueueTailBytesLeft = 0;
                }
                break;
            }

            if (sendQueueTailBytesLeft < it->size()) {
                if (sendQueueTailBytesLeft > 0) {
                    sendQueue.back().trim(RDMAStack::RCDataSize-sendQueueTailBytesLeft);
                }
                sendQueue.emplace_back(RDMAStack::RCDataSize);
                sendQueueTailBytesLeft = RDMAStack::RCDataSize;
            }

            memcpy(sendQueue.back().get_write()+(RDMAStack::RCDataSize-sendQueueTailBytesLeft),
                    it->get(), it->size());
            sendQueueTailBytesLeft -= it->size();
        }

        if (it != buf.end()) {
            sendQueue.insert(sendQueue.end(), std::make_move_iterator(it),
                            std::make_move_iterator(buf.end()));
            sendQueueTailBytesLeft = 0;
        }

        stack->sendQueueSize += sendQueue.size() - beforeSize;
        return;
    }

    processSends<std::vector<Buffer>>(buf);
    if (buf.size()) {
        stack->sendQueueSize += buf.size();
        sendQueue.insert(sendQueue.end(), std::make_move_iterator(buf.begin()),
                         std::make_move_iterator(buf.end()));
    }
}

void RDMAConnection::shutdownConnection() {
    if (errorState) {
        return;
    }

    errorState = true;

    if (recvPromiseActive) {
        recvPromiseActive = false;
        recvPromise.set_value(Buffer());
    }

    if (sendQueue.size()) {
        K2WARN("Shutting down RC QP with pending sends");
    }

    if (QP) {
        (void) engine()._thread_pool->submit<int>([QP=this->QP]() {
            // Transitioning the QP into the Error state will flush
            // any outstanding WRs, possibly with errors. Is needed to
            // maintain consistency in the SRQ
            struct ibv_qp_attr attr = {};
            attr.qp_state = IBV_QPS_ERR;

            ibv_modify_qp(QP, &attr, IBV_QP_STATE);
            ibv_destroy_qp(QP);

            return 0;
        });
        QP = nullptr;
    }

    if (isReady) {
        stack->RCConnectionCount--;
        isReady = false;
    }

    // Connection will be removed from RCLookup and/or handshakeLookup
    // when they are referenced there and the weak_ptr is null
}

RDMAConnection::~RDMAConnection() noexcept {
    shutdownConnection();
}

int initRDMAContext(const std::string& RDMADeviceName) {
    if (initialized) {
        return 0;
    }

    int numDevices;
    struct ibv_device** devices = ibv_get_device_list(&numDevices);
    if (!devices) {
        K2ERROR("ibv_get_device_list failed: " << strerror(errno));
        return -1;
    }

    int deviceIdx=0;
    for(; deviceIdx < numDevices; ++deviceIdx) {
        if (strcmp(ibv_get_device_name(devices[deviceIdx]), RDMADeviceName.c_str()) == 0) {
            break;
        }
    }

    if (deviceIdx == numDevices) {
        ibv_free_device_list(devices);
        K2WARN("No suitable RDMA device found");
        return -1;
    }

    K2DEBUG("Using RDMA device: " << ibv_get_device_name(devices[deviceIdx]));

    ctx = ibv_open_device(devices[deviceIdx]);
    K2ASSERT(ctx, "ibv_open_device failed");

    struct ibv_device_attr attr;
    ibv_query_device(ctx, &attr);
    K2DEBUG("RDMA maximum queue pairs: " << attr.max_qp);

    ibv_free_device_list(devices);

    initialized = true;
    return 0;
}

RDMAListener RDMAStack::listen() {
    return RDMAListener(this);
}

RDMAStack::~RDMAStack() {
    for (auto it = RCLookup.begin(); it != RCLookup.end(); ++it) {
        if (it->second) {
            it->second->shutdownConnection();
        }
    }

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

    if (acceptPromiseActive) {
        acceptPromise.set_exception(std::runtime_error("RDMAStack destroyed with accept promise active"));
    }

    for (int i=0; i<RecvWRData::maxWR; ++i) {
        struct ibv_sge& SG = UDQPRRs.Segments[i];
        free((void*)SG.addr);
        struct ibv_sge& SRQSG = RCQPRRs.Segments[i];
        free((void*)SRQSG.addr);
    }
}

void RDMAStack::sendHandshakeResponse(const EndPoint& endpoint, uint32_t QPNum, uint32_t id) {
    Buffer response(sizeof(UDMessage));
    UDMessage* message = (UDMessage*)response.get();
    message->op = UDOps::HandshakeResponse;
    message->QPNum = QPNum;
    message->requestId = id;
    sendUDQPMessage(std::move(response), endpoint.GID, endpoint.UDQP);
}

uint32_t RDMAStack::sendHandshakeRequest(const EndPoint& endpoint, uint32_t QPNum) {
    Buffer response(sizeof(UDMessage));
    UDMessage* message = (UDMessage*)response.get();
    message->op = UDOps::HandshakeRequest;
    message->QPNum = QPNum;
    uint32_t id = handshakeId++;
    message->requestId = id;
    sendUDQPMessage(std::move(response), endpoint.GID, endpoint.UDQP);
    return id;
}

future<struct ibv_ah*> RDMAStack::getAH(const union ibv_gid& GID) {
    auto AHIt = AHLookup.find(GID);
    if (AHIt != AHLookup.end()) {
        return make_ready_future<struct ibv_ah*>(AHIt->second);
    }

    return engine()._thread_pool->submit<struct ibv_ah*>([GID, stack=weak_from_this()]() {
        struct ibv_ah* AH = nullptr;
        if (!stack) {
            return AH;
        }

        struct ibv_ah_attr AHAttr;
        fillAHAttr(AHAttr, GID);

        AH = ibv_create_ah(stack->protectionDomain, &AHAttr);
        if (!AH) {
            return AH;
        }

        stack->AHLookup[GID] = AH;
        return AH;
    });
}

int RDMAStack::sendUDQPMessage(Buffer buffer, const union ibv_gid& destGID, uint32_t destQP) {
    K2ASSERT(buffer.size() + UDDataOffset <= UDQPRxSize, "UD Message too large");

    (void) getAH(destGID).then([destQP, buff=std::move(buffer), stack=weak_from_this()] (struct ibv_ah* AH) mutable {
        if (!stack || !AH) {
            K2WARN("Failed to create RDMA address handle!");
            return;
        }

        int idx = stack->trySendUDQPMessage(buff, AH, destQP);
        if (idx < 0) {
            stack->UDSendQueue.emplace_back(std::move(buff), AH, destQP);
        } else {
            stack->UDOutstandingBuffers[idx] = std::move(buff);
        }
    });

    return 0;
}

int RDMAStack::trySendUDQPMessage(const Buffer& buffer, struct ibv_ah* AH, uint32_t destQP) {
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
    if (int err = ibv_post_send(UDQP, SR, &badSR)) {
        K2ASSERT(false, "Failed to post UD send: " << strerror(err));
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
    if (int err = ibv_post_send(UDQP, firstSR, &badSR)) {
        K2ASSERT(false, "Failed to post UD send: " << strerror(err));
        return false;
    }

    UDQPSRs.postedCount += toProcess;
    UDSendQueue.erase(UDSendQueue.cbegin(), UDSendQueue.cbegin()+toProcess);
    return true;
}

void RDMAStack::processCompletedSRs(std::array<Buffer, SendWRData::maxWR>& buffers, SendWRData& WRData, uint64_t signaledID) {
    uint32_t freed=0;
    K2ASSERT(WRData.postedIdx <= signaledID, "Send assumptions bad");
    for (int i=WRData.postedIdx; i<=(int)signaledID; ++i, ++freed) {
        buffers[i] = Buffer();
    }

    WRData.postedIdx = (WRData.postedIdx + freed) % SendWRData::maxWR;
    K2ASSERT(freed <= WRData.postedCount, "Bug in circular buffer for SRs");
    WRData.postedCount -= freed;
}

future<std::unique_ptr<RDMAConnection>> RDMAStack::accept() {
    if (acceptPromiseActive) {
        return make_exception_future<std::unique_ptr<RDMAConnection>>(std::runtime_error("accept() called while accept future still active"));
    }

    if (acceptQueue.size()) {
        auto accept_future = make_ready_future<std::unique_ptr<RDMAConnection>>(std::move(acceptQueue.front()));
        acceptQueue.pop_front();
        return accept_future;
    }

    acceptPromiseActive = true;
    acceptPromise = promise<std::unique_ptr<RDMAConnection>>();
    return acceptPromise.get_future();
}

std::unique_ptr<RDMAConnection> RDMAStack::connect(const EndPoint& remote) {
    std::unique_ptr<RDMAConnection> conn = std::make_unique<RDMAConnection>(this, remote);
    conn->makeHandshakeRequest();
    return std::move(conn);
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
        uint32_t id = message->requestId;
        auto connIt = handshakeLookup.find(id);
        if (connIt == handshakeLookup.end()) {
            K2WARN("Unsolicited Handshake response");
            return;
        }
        else if (!connIt->second) {
            K2WARN("Connection deleted before handshake completed");
            handshakeLookup.erase(connIt);
            return;
        }

        (void) connIt->second->completeHandshake(message->QPNum).
        then([this, id] () {
            auto eraseIt = handshakeLookup.find(id);
            if (eraseIt != handshakeLookup.end()) {
                handshakeLookup.erase(eraseIt);
            }
        });
    } else {
        K2ASSERT(false, "Unknown UD op");
    }
}

bool RDMAStack::processRCCQ() {
    struct ibv_wc WCs[pollBatchSize];
    int completed = ibv_poll_cq(RCCQ, pollBatchSize, WCs);
    K2ASSERT(completed >= 0, "Failed to poll RC CQ");

    if (completed == 0) {
        return false;
    }

    int recvWCs = 0;
    struct ibv_recv_wr* prevRR = nullptr;
    struct ibv_recv_wr* firstRR = nullptr;

    for (int i=0; i<completed; ++i) {
        bool foundConn = true;
        auto connIt = RCLookup.find(WCs[i].qp_num);
        if (connIt == RCLookup.end()) {
            K2WARN("RCQP not found");
            foundConn = false;
        }
        if (foundConn && !connIt->second) {
            // This is the normal case for how connections are removed
            // from RCLookup
            K2DEBUG("RDMAConnection for RCQP was deleted");
            RCLookup.erase(connIt);
            foundConn = false;
        }

        // WC op code is not valid if there was an error, so we use the
        // wr_id to differentiate between sends and receives
        bool isRecv = WCs[i].wr_id < RecvWRData::maxWR;

        if (!isRecv) {
            // We can ignore send completions for connections that no longer exist
            if (!foundConn || !connIt->second->QP) {
                continue;
            }

            processCompletedSRs(connIt->second->outstandingBuffers,
                                connIt->second->sendWRs, WCs[i].wr_id - RecvWRData::maxWR);
            if (WCs[i].status != IBV_WC_SUCCESS) {
                K2WARN("error on send wc: " << ibv_wc_status_str(WCs[i].status));
                connIt->second->shutdownConnection();
            }

            size_t beforeSize = connIt->second->sendQueue.size();
            connIt->second->processSends(connIt->second->sendQueue);
            size_t afterSize = connIt->second->sendQueue.size();
            sendQueueSize -= beforeSize - afterSize;
        } else {
            int idx = WCs[i].wr_id;
            struct ibv_recv_wr& RR = RCQPRRs.RecvRequests[idx];
            ++recvWCs;

            if (WCs[i].status != IBV_WC_SUCCESS) {
                K2WARN("error on recv wc: " << ibv_wc_status_str(WCs[i].status));
                if (foundConn) {
                    connIt->second->shutdownConnection();
                }
                free((char*)RCQPRRs.Segments[idx].addr);
            } else if (foundConn) {
                char* data = (char*)RCQPRRs.Segments[idx].addr;
                uint32_t size = WCs[i].byte_len;
                connIt->second->incomingMessage(data, size);
            }

            // Prepare RR to be posted again. We cannot assume RRs are completed in order
            if (!firstRR) {
                firstRR = &RR;
            }
            if (prevRR) {
                prevRR->next = &RR;
            }
            prevRR = &RR;
            RCQPRRs.Segments[idx].addr = (uint64_t)malloc(RCDataSize);
            K2ASSERT(RCQPRRs.Segments[idx].addr, "Failed to allocate memory for RR");
        }
    }

    if (recvWCs) {
        totalRecv += recvWCs;
        recvBatchSum += recvWCs;
        ++recvBatchCount;

        prevRR->next = nullptr;
        struct ibv_recv_wr* badRR;
        if (int err = ibv_post_srq_recv(SRQ, firstRR, &badRR)) {
            K2ASSERT(false, "error on RC post_recv: " << strerror(err));
        }
    }

    return true;

}

bool RDMAStack::processUDCQ() {
    struct ibv_wc WCs[pollBatchSize];
    int completed = ibv_poll_cq(UDCQ, pollBatchSize, WCs);
    K2ASSERT(completed >= 0, "Failed to poll UD CQ");

    if (completed == 0) {
        return false;
    }

    for (int i=0; i<completed; ++i) {
        K2ASSERT(WCs[i].status == IBV_WC_SUCCESS, "Error on UDQP WR");

        if (WCs[i].opcode == IBV_WC_SEND) {
            processCompletedSRs(UDOutstandingBuffers, UDQPSRs, WCs[i].wr_id);
        } else {
            int idx = WCs[i].wr_id;

            UDMessage* message = (UDMessage*)(UDQPRRs.Segments[idx].addr+UDDataOffset);
            struct ibv_grh* grh = (struct ibv_grh*)UDQPRRs.Segments[idx].addr;
            processUDMessage(message, EndPoint(grh->sgid, WCs[i].src_qp));

            UDQPRRs.RecvRequests[idx].next = nullptr;
            struct ibv_recv_wr* badRR;
            if (int err = ibv_post_recv(UDQP, &(UDQPRRs.RecvRequests[idx]), &badRR)) {
                K2ASSERT(false, "error on UD post_recv: " << strerror(err));
            }
        }
    }

    return true;
}

bool RDMAStack::poller() {
    if (!UDCQ) {
        return false;
    }

    sendQueueSum += sendQueueSize;
    ++sendQueueCount;

    bool didWork = processUDCQ();

    didWork |= processUDSendQueue();
    didWork |= processRCCQ();

    return didWork;
}

void RDMAStack::registerPoller() {
    RDMAPoller = reactor::poller::simple([&] { return poller(); });
}

void RDMAStack::registerMetrics() {
    namespace sm = seastar::metrics;

    metricGroup.add_group("rdma_stack", {
        sm::make_counter("send_count", totalSend,
            sm::description("Total number of messages sent")),
        sm::make_counter("recv_count", totalRecv,
            sm::description("Total number of messages received")),
        sm::make_gauge("send_queue_size", [this]{
                if (!sendQueueCount) {
                    return 0.0;
                }
                double avg = sendQueueSum / (double)sendQueueCount;
                sendQueueSum = sendQueueCount = 0;
                return avg;
            },
            sm::description("Average size of the send queue")),
        sm::make_gauge("send_batch_size", [this]{
                if (!sendBatchCount) {
                    return 0.0;
                }
                double avg = sendBatchSum / (double)sendBatchCount;
                sendBatchSum = sendBatchCount = 0;
                return avg;
            },
            sm::description("Average size of the send batches")),
        sm::make_gauge("recv_batch_size", [this]{
                if (!recvBatchCount) {
                    return 0.0;
                }
                double avg = recvBatchSum / (double)recvBatchCount;
                recvBatchSum = recvBatchCount = 0;
                return avg;
            },
            sm::description("Average size of receive batches"))
    });
}

std::unique_ptr<RDMAStack> RDMAStack::makeUDQP(std::unique_ptr<RDMAStack> stack) {
    // Step 1. Create a CQ
    stack->UDCQ = ibv_create_cq(ctx, RecvWRData::maxWR+SendWRData::maxWR, nullptr, nullptr, 0);
    if (!stack->UDCQ) {
        K2ERROR("failed to create UDCQ: " << strerror(errno));
        return nullptr;
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
        K2ERROR("Failed to create UD QP: " << strerror(errno));
        return nullptr;
    }

    // Step 3. Transition QP into INIT state
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = 1;
    if (int err = ibv_modify_qp(stack->UDQP, &attr,
                               IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY)) {
        K2ERROR("Failed to transition UD QP into init state: " << strerror(err));
        return nullptr;
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
        // This memory will be freed when the RDMAStack is destroyed
        SG.addr = (uint64_t)malloc(UDQPRxSize);
        K2ASSERT(SG.addr, "Failed to allocate memory for RR");
        SG.length = UDQPRxSize;
        SG.lkey = stack->memRegionHandle->lkey;
        RR.sg_list = &SG;
        RR.num_sge = 1;
    }
    struct ibv_recv_wr* badRR;
    if (int err = ibv_post_recv(stack->UDQP, stack->UDQPRRs.RecvRequests, &badRR)) {
        K2ERROR("Failed to post UD QP RRs: " << strerror(err));
        return nullptr;
    }

    // Step 5. Transition QP into ready-to-receive (RTR) state
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTR;
    if (int err = ibv_modify_qp(stack->UDQP, &attr, IBV_QP_STATE)) {
        K2ERROR("Failed to transition UD QP into RTR: " << strerror(err));
        return nullptr;
    }

    // Step 6. Transition QP into ready-to-send (RTS) state
    memset(&attr, 0, sizeof(ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 0;
    if (int err = ibv_modify_qp(stack->UDQP, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN)) {
        K2ERROR("Failed to transition UD QP into RTS: " << strerror(err));
        return nullptr;
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
        return nullptr;
    }

    std::unique_ptr<RDMAStack> stack = std::make_unique<RDMAStack>();

    stack->protectionDomain = ibv_alloc_pd(ctx);
    if (!stack->protectionDomain) {
        K2ERROR("ibv_alloc_pd failed");
        return nullptr;
    }

    stack->memRegionHandle = ibv_reg_mr(stack->protectionDomain, memRegion, memRegionSize, IBV_ACCESS_LOCAL_WRITE);
    if (!stack->memRegionHandle) {
        K2ERROR("Failed to register memory: " << strerror(errno));
        return nullptr;
    }

    //Port 1 index 1 should be our ROCEv2 gid
    ibv_query_gid(ctx, 1, 1, &(stack->localEndpoint.GID));

    stack = makeUDQP(std::move(stack));
    if (!stack) {
        return stack;
    }

    stack->RCCQ = ibv_create_cq(ctx, RCCQSize, nullptr, nullptr, 0);
    if (!stack->RCCQ) {
        K2ERROR("Failed to create RCCQ: " << strerror(errno));
        return nullptr;
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
        K2ERROR("Failed to create SRQ: " << strerror(errno));
        return nullptr;
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
        // When an incoming message is received, this malloc'ed memory
        // will be wrapped in a temporary_buffer and passed to the user
        // so it will be freed when the user drops the temporary_buffer
        SG.addr = (uint64_t)malloc(RCDataSize);
        K2ASSERT(SG.addr, "Failed to allocate memory for RR");
        SG.length = RCDataSize;
        SG.lkey = stack->memRegionHandle->lkey;
        RR.sg_list = &SG;
        RR.num_sge = 1;
    }
    struct ibv_recv_wr* badRR;
    if (int err = ibv_post_srq_recv(stack->SRQ, stack->RCQPRRs.RecvRequests, &badRR)) {
        K2ERROR("failed to post SRQ RRs: " << strerror(err));
        return nullptr;
    }

    stack->registerPoller();
    stack->registerMetrics();

    return stack;
}

sstring EndPoint::GIDToString(union ibv_gid gid) {
    char buffer[INET6_ADDRSTRLEN];
    buffer[0] = '\0';
    static_assert(sizeof(struct in6_addr) == sizeof(union ibv_gid));
    // parse the gid as an ipv6
    ::inet_ntop(AF_INET6, &gid.raw, buffer, INET6_ADDRSTRLEN);
    return sstring(buffer);
}

int EndPoint::StringToGID(const sstring& strGID, union ibv_gid& result) {
    static_assert(sizeof(struct in6_addr) == sizeof(union ibv_gid));
    // use ipv6 parser
    if (::inet_pton(AF_INET6, strGID.c_str(), &result.raw) != 1) {
        return -1;
    }
    return 0;
}

} // namespace rdma
} // namespace seastar
