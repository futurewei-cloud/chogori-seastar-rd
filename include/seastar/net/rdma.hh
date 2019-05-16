#pragma once

#include <deque>
#include <unordered_map>
#include <array>
#include <functional>

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/core/weak_ptr.hh>

#include <infiniband/verbs.h>

bool operator==(const union ibv_gid& lhs, const union ibv_gid& rhs);

namespace seastar { namespace rdma { class EndPoint; }}

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

template <>
struct hash<seastar::rdma::EndPoint> {
    std::size_t operator()(const seastar::rdma::EndPoint&) const;
};
   
}

namespace seastar {
namespace rdma {

class RDMAStack;

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

    SendWRData() = default;
    SendWRData(SendWRData&&) = default;
    SendWRData& operator=(SendWRData&&) = default;
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
    EndPoint() = default;
    bool operator==(const EndPoint& rhs) const {
        return (GID == rhs.GID) && (UDQP == rhs.UDQP);
    }
};

class RDMAConnection : public weakly_referencable<RDMAConnection> {
public:
    future<temporary_buffer<uint8_t>&&> recv();
    void send(std::vector<temporary_buffer<uint8_t>>&& buf);

    RDMAConnection(RDMAStack* stack, EndPoint remote) : 
        stack(stack), remote(remote) {}
    ~RDMAConnection() noexcept;
    RDMAConnection(RDMAConnection&&) noexcept;
    RDMAConnection& operator=(RDMAConnection&&) noexcept;
private:
    bool isReady = false;
    std::deque<temporary_buffer<uint8_t>> recvQueue;
    std::deque<temporary_buffer<uint8_t>> sendQueue;

    SendWRData SendWRs;
    std::array<temporary_buffer<uint8_t>, SendWRData::maxWR> outstandingBuffers;
    template <class VecType>
    bool processSends(VecType& queue);

    struct ibv_qp* QP = nullptr;
    RDMAStack* stack = nullptr;
    EndPoint remote;

    bool recvPromiseActive = false;
    promise<temporary_buffer<uint8_t>&&> recvPromise;

    void handshakeResponse(uint32_t remoteQP);
    void handshakeRequest(uint32_t remoteQP);

    RDMAConnection() = delete;

    friend class RDMAStack;
};

class RDMAStack {
public:
    union ibv_gid myGID;

    future<RDMAConnection> accept();
    RDMAConnection connect(EndPoint endpoint);

    static std::unique_ptr<RDMAStack> makeRDMAStack(void* memRegion, size_t memRegionSize);
    RDMAStack() = default;
    ~RDMAStack() noexcept;

    bool poller();
private:
    compat::optional<reactor::poller> RDMAPoller;
    struct ibv_pd* protectionDomain = nullptr;
    struct ibv_mr* memRegionHandle = nullptr;
    struct ibv_qp* UDQP = nullptr;
    struct ibv_cq* UDCQ = nullptr;

    enum class UDOps : uint32_t {
        HandshakeRequest,
        HandshakeResponse,
    };
    struct UDMessage {
        UDOps op;
        uint32_t remoteQP;
    };
    // There is a 40 byte overhead for UD
    static constexpr size_t UDQPRxSize = sizeof(UDMessage) + 40;
    RecvWRData UDQPRRs;
    SendWRData UDQPSRs;
    std::array<temporary_buffer<uint8_t>, SendWRData::maxWR> UDOutstandingBuffers;
    std::deque<UDSend> UDSendQueue;

    int sendUDQPMessage(temporary_buffer<uint8_t> buffer, const union ibv_gid& GID, uint32_t destQP);
    int trySendUDQPMessage(const temporary_buffer<uint8_t>& buffer, struct ibv_ah* AH, uint32_t destQP);
    void processUDMessage(UDMessage*, EndPoint);
    bool processUDSendQueue();
    bool processUDCQ();
    void freeUDSRs(uint64_t WRID);

    static void fillAHAttr(struct ibv_ah_attr& AHAttr, const union ibv_gid& GID);
    std::unordered_map<union ibv_gid, struct ibv_ah*> AHLookup;
    std::unordered_map<EndPoint, weak_ptr<RDMAConnection>> connectionLookup;
    static std::unique_ptr<RDMAStack> makeUDQP(std::unique_ptr<RDMAStack> stack);

    static constexpr int maxExpectedConnections = 1024;
    static constexpr int RCCQSize = RecvWRData::maxWR + 
            (maxExpectedConnections * (SendWRData::maxWR / SendWRData::signalThreshold));
    RecvWRData RCQPRRs;
    struct ibv_cq* RCCQ = nullptr;
    struct ibv_srq* SRQ = nullptr;
    std::unordered_map<uint32_t, weak_ptr<RDMAConnection>> RCLookup;
    int RCConnectionCount = 0;

    void registerPoller();

    bool acceptPromiseActive = false;
    promise<RDMAConnection> acceptPromise;
    std::vector<RDMAConnection> acceptQueue;

    //TODO move to slow core
    struct ibv_ah* makeAH(const union ibv_gid& GID);

    friend class RDMAConnection;
};

} // namespace rdma
} // namespace seastar

