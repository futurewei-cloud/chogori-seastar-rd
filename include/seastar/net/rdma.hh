#pragma once

#include <deque>
#include <unordered_map>
#include <array>
#include <functional>

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>

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
    static_assert(maxWR % signalThreshold == 0);
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

struct EndPoint {
    union ibv_gid GID;
    uint32_t UDQP;
    EndPoint(union ibv_gid gid, uint32_t qp) :
        GID(gid), UDQP(qp) {}
    bool operator==(const EndPoint& lhs, const EndPoint& rhs) {
        return (lhs.GID == rhs.GID) && (lhs.UDQP == rhs.UDQP);
    }
};

class RDMAConnection : weakly_referencable<RDMAConnection> {
public:
    future<temporary_buffer<uint8_t>&&> recv();
    void send(std::vector<temporary_buffer<uint8_t>>&& buf);

    RDMAConnection(RDMAStack* stack, EndPoint remote) : 
        stack(stack), remote(remote) {}
    ~RDMAConnection();
    RDMAConnection(RDMAConnection&&) = default;
    RDMAConnection& operator=(RDMAConnection&&) = default;
private:
    bool isReady = false;
    deque<temporary_buffer<uint8_t>> recvQueue;
    deque<temporary_buffer<uint8_t>> sendQueue;
    EndPoint remote;

    SendWRData SendWRs;
    std::array<temporary_buffer<uint8_t>, SendWRData::maxWR> outstandingBuffers;
    template <class VecType>
    bool processSends(VecType& queue);

    struct ibv_qp* QP;
    RDMAStack* stack;

    bool recvPromiseActive = false;
    promise<temporary_buffer<uint8_t>&&> recvPromise;

    void handshakeResponse(uint32_t remoteQP);

    RDMAConnection() = delete;
};

class RDMAStack {
public:
    union ibv_gid myGID;

    future<RDMAConnection> accept();
    RDMAConnection connect(EndPoint endpoint);

    static std::unique_ptr<RDMAStack> makeRDMAStack(void* memRegion, size_t memRegionSize);
    RDMAStack() = default;
    ~RDMAStack();

    bool poller();
private:
    compat::optional<reactor::poller> RDMAPoller;
    struct ibv_pd* protectionDomain = nullptr;
    struct ibv_mr* memRegionHandle = nullptr;
    struct ibv_qp* UDQP = nullptr;
    struct ibv_cq* UDCQ = nullptr;
    struct ibv_cq* RCCQ = nullptr;
    struct ibv_srq* SRQ = nullptr;

    enum class UDOps : uint32_t {
        HandshakeRequest,
        HandshakeResponse,
    };
    struct UDMessage {
        UDOps op;
        uint32_t RemoteQP;
    };
    // There is a 40 byte overhead for UD
    static constexpr size_t UDQPRxSize = sizeof(UDMessage) + 40;
    RecvWRData UDQPRRs;
    SendWRData UDQPSRs;

    std::array<temporary_buffer<uint8_t>, SendWRData::maxWR> UDOutstandingBuffers;
    std::deque<UDSend> UDSendQueue;
    std::unordered_map<union ibv_gid, struct ibv_ah*> AHLookup;
    std::unordered_map<EndPoint, weak_ptr<RDMAConnection>> connectionLookup;
    static std::unique_ptr<RDMAStack> makeUDQP(std::unique_ptr<RDMAStack> stack);

    int sendUDQPMessage(temporary_buffer<uint8_t> buffer, const union ibv_gid& GID, uint32_t destQP);
    int trySendUDQPMessage(const temporary_buffer<uint8_t>& buffer, struct ibv_ah* AH, uint32_t destQP);

    bool processUDSendQueue();
    bool processUDCQ();
    void freeUDSRs(uint64_t WRID);
    void registerPoller();

    bool acceptPromiseActive = false;
    promise<RDMAConnection> acceptPromise;
    std::vector<RDMAConnection> acceptQueue;

    //TODO move to slow core
    struct ibv_ah* makeAH(const union ibv_gid& GID);
};

} // namespace rdma
} // namespace seastar

namespace std {

template <>
struct hash<EndPoint>
{
    // TODO investigate smarter hash algorithms
    std::size_t operator()(const EndPoint& endpoint) const {
        return std::hash{}(endpoint.GID) ^ std::hash{}(endpoint.UDQP);
    }
};
    
}

