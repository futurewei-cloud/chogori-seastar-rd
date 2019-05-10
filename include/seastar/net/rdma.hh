#pragma once

#include <deque>

#include <infiniband/verbs.h>

namespace seastar {
namespace rdma {

int initRDMAContext();

struct RecvWRData {
    static constexpr uint32_t maxWR = 128;
    struct ibv_recv_wr RecvRequests[maxWR];
    struct ibv_sge Segments[maxWR];
    uint32_t unusedIdx = 0;
    uint32_t unusedCount = 0;
};

struct SendWRData {
    static constexpr uint32_t maxWR = 128;
    static constexpr uint32_t signalThreshold = 32;
    struct ibv_send_wr SendRequests[maxWR];
    struct ibv_sge Segments[maxWR];
    uint32_t postedIdx = 0;
    uint32_t postedCount = 0;
};

struct UDSend {
    char* data;
    struct ibv_ah* AH;
    int size;
    uint32_t destQP;
    UDSend (char* d, struct ibv_ah* ah, int s, uint32_t qp) : 
        data(d), AH(ah), size(s), destQP(qp) {}
    UDSend() = delete;
};

class RDMAStack {
public:
    union ibv_gid myGID;

    static std::unique_ptr<RDMAStack> RDMAStack::makeRDMAStack(void* memRegion, size_t memRegionSize);
    int sendUDQPMessage(char* data, int size, struct ibv_ah* AH, uint32_t destQP);
    ~RDMAStack();

    bool poller();
private:
    reactor::poller poller;
    // There is a 40 byte overhead for UD, so 64 should still give us plenty of space
    static constexpr size_t UDQPRxSize = 64; 
    RecvWRData UDQPRRs;
    SendWRData UDQPSRs;
    std::deque<UDSend> UDQPSendQueue;
    RDMAStack() = default;
    static std::unique_ptr<RDMAStack> makeControlQP(std::unique_ptr<RDMAStack> stack);
    int trySendUDQPMessage(char* data, int size, struct ibv_ah* AH, uint32_t destQP);
};

}
}
