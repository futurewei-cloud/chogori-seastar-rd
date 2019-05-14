#pragma once

#include <deque>
#include <unordered_map>
#include <array>
#include <functional>

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/reactor.hh>

#include <infiniband/verbs.h>

bool operator==(const union ibv_gid& lhs, const union ibv_gid& rhs) {
    return (memcmp(lhs.raw, rhs.raw, 16) == 0);
}

namespace std {

template <>
struct hash<union ibv_gid>
{
    // TODO investigate smarter hash algorithms
    std::size_t operator()(const union ibv_gid& gid) const {
        size_t half1 = *(size_t*)gid.raw;
        size_t half2 = *(size_t*)(gid.raw + 8);
        return std::hash<unsigned long long>{}((unsigned long long)(half1 ^ half2));
    }
};
    
}


namespace seastar {

class reactor;

namespace rdma {

int initRDMAContext();

struct RecvWRData {
    static constexpr int maxWR = 128;
    struct ibv_recv_wr RecvRequests[maxWR];
    struct ibv_sge Segments[maxWR];
    uint32_t unusedIdx = 0;
    uint32_t unusedCount = 0;
};

struct SendWRData {
    static constexpr int maxWR = 128;
    static constexpr int signalThreshold = 32;
    struct ibv_send_wr SendRequests[maxWR];
    struct ibv_sge Segments[maxWR];
    uint32_t postedIdx = 0;
    uint32_t postedCount = 0;
};

struct UDSend {
    temporary_buffer<uint8_t> buffer;
    struct ibv_ah* AH;
    uint32_t destQP;
    UDSend (temporary_buffer<uint8_t>&& buf, struct ibv_ah* ah, uint32_t qp) : 
        buffer(std::move(buf)), AH(ah), destQP(qp) {}
    UDSend() = delete;
};

class RDMAStack {
public:
    union ibv_gid myGID;

    static std::unique_ptr<RDMAStack> makeRDMAStack(void* memRegion, size_t memRegionSize);
    int sendUDQPMessage(temporary_buffer<uint8_t> buffer, const union ibv_gid& GID, uint32_t destQP);
    RDMAStack() = default;
    ~RDMAStack();

    bool poller();
private:
    compat::optional<reactor::poller> RDMAPoller;
    struct ibv_pd* protectionDomain;
    struct ibv_mr* memRegionHandle;
    struct ibv_qp* UDQP;
    struct ibv_cq* UDCQ;

    enum class UDOps : uint32_t {
        ConnectRequest,
        ConnectResponse,
        Data
    };
    // There is a 40 byte overhead for UD, so 64 should still give us plenty of space
    static constexpr size_t UDQPRxSize = 64; 
    RecvWRData UDQPRRs;
    SendWRData UDQPSRs;
    std::array<temporary_buffer<uint8_t>, SendWRData::maxWR> UDOutstandingBuffers;
    std::deque<UDSend> UDSendQueue;
    std::unordered_map<union ibv_gid, struct ibv_ah*> AHLookup;
    //std::unordered_map<std::pair<union ibv_gid, uint32_t>, RDMAConnection*> connectionLookup;
    static std::unique_ptr<RDMAStack> makeUDQP(std::unique_ptr<RDMAStack> stack);
    int trySendUDQPMessage(const temporary_buffer<uint8_t>& buffer, struct ibv_ah* AH, uint32_t destQP);
    bool processUDSendQueue();
    bool processUDCQ();
    void freeUDSRs(uint64_t WRID);
    void registerPoller();

    //TODO move to slow core
    struct ibv_ah* makeAH(const union ibv_gid& GID);
};

} // namespace rdma
} // namespace seastar


