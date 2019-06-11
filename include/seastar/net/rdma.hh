//
//  (C)opyright Futurewei Technologies Inc, 2019
//

#pragma once

#include <deque>
#include <unordered_map>
#include <array>
#include <functional>
#include <string>

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/core/sstring.hh>

#include <infiniband/verbs.h>

bool operator==(const union ibv_gid& lhs, const union ibv_gid& rhs);
namespace seastar { namespace rdma { class EndPoint; }}

namespace std {

template <>
struct hash<union ibv_gid>
{
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
class RDMAListener;

// Opens the RDMA device context, which is shared by all cores/RDMAStacks. The verbs API
// is thread safe so sharing it is ok. Should only be called once at program start
int initRDMAContext(const std::string& RDMADeviceName);

struct RecvWRData {
    static constexpr int maxWR = 256;
    struct ibv_recv_wr RecvRequests[maxWR];
    struct ibv_sge Segments[maxWR];
};

struct SendWRData {
    static constexpr int maxWR = 32;
    static constexpr int signalThreshold = 16;
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
    EndPoint(const sstring& strGID, uint32_t qp): GID{0}, UDQP(0) {
        union ibv_gid gid{0};
        if (StringToGID(strGID, gid) == 0) {
            GID = gid;
            UDQP = qp;
        }
    }
    EndPoint() = default;
    bool operator==(const EndPoint& rhs) const {
        return (GID == rhs.GID) && (UDQP == rhs.UDQP);
    }
    static sstring GIDToString(union ibv_gid gid);
    static int StringToGID(const sstring& strGID, union ibv_gid& result);
};

class RDMAConnectionError : std::exception {};

class RDMAConnection : public weakly_referencable<RDMAConnection> {
public:
    future<temporary_buffer<uint8_t>> recv();
    void send(std::vector<temporary_buffer<uint8_t>>&& buf);

    RDMAConnection(RDMAStack* stack, EndPoint remote) :
        stack(stack), remote(remote) {}
    ~RDMAConnection() noexcept;
    RDMAConnection(RDMAConnection&&) = delete;
    RDMAConnection& operator=(RDMAConnection&&) = delete;
    bool closed() {
        return errorState;
    }
    future<> close() {
        if (errorState) {
            return make_ready_future<>();
        }

        sendCloseSignal();
        closePromiseActive = true;
        closePromise = promise<>();
        return closePromise.get_future();
    }
    const EndPoint& getAddr() const {
        return remote;
    }
private:
    bool isReady = false;
    bool errorState = false;
    std::deque<temporary_buffer<uint8_t>> recvQueue;

    // Messages below this size will be copied into packed RDMA sends
    // if possible
    static constexpr int messagePackThreshold = 200;
    // The number of bytes allocated but not used in the last temporary_buffer
    // of the sendQueue. Used to pack new messages into fewer send requests
    size_t sendQueueTailBytesLeft = 0;
    std::deque<temporary_buffer<uint8_t>> sendQueue;

    SendWRData sendWRs;
    std::array<temporary_buffer<uint8_t>, SendWRData::maxWR> outstandingBuffers;
    template <class VecType>
    void processSends(VecType& queue);
    void incomingMessage(unsigned char* data, uint32_t size);
    void sendCloseSignal();

    struct ibv_qp* QP = nullptr;
    RDMAStack* stack = nullptr;
    EndPoint remote;

    bool closePromiseActive = false;
    promise<> closePromise;

    bool recvPromiseActive = false;
    promise<temporary_buffer<uint8_t>> recvPromise;

    void makeQP();
    void makeHandshakeRequest();
    void completeHandshake(uint32_t remoteQP);
    void processHandshakeRequest(uint32_t remoteQP, uint32_t responseId);

    void shutdownConnection();

    RDMAConnection() = delete;

    friend class RDMAStack;
};

class RDMAStack {
public:
    EndPoint localEndpoint;

    future<std::unique_ptr<RDMAConnection>> accept();
    std::unique_ptr<RDMAConnection> connect(const EndPoint& remote);

    static std::unique_ptr<RDMAStack> makeRDMAStack(void* memRegion, size_t memRegionSize);
    RDMAStack() = default;
    ~RDMAStack() noexcept;

    bool poller();
    RDMAListener listen();

    static constexpr uint32_t RCDataSize = 8192;
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
        uint32_t QPNum;
        uint32_t requestId;
    };
    // There is a 40 byte overhead for incoming UD messages
    // The first 40 bytes contains headers that indicate the source of the message
    static constexpr uint32_t UDDataOffset = 40;
    static constexpr size_t UDQPRxSize = sizeof(UDMessage) + 40;
    RecvWRData UDQPRRs;
    SendWRData UDQPSRs;
    std::array<temporary_buffer<uint8_t>, SendWRData::maxWR> UDOutstandingBuffers;
    std::deque<UDSend> UDSendQueue;

    int sendUDQPMessage(temporary_buffer<uint8_t> buffer, const union ibv_gid& GID, uint32_t destQP);
    int trySendUDQPMessage(const temporary_buffer<uint8_t>& buffer, struct ibv_ah* AH, uint32_t destQP);
    void processUDMessage(UDMessage*, EndPoint);
    void sendHandshakeResponse(const EndPoint& endpoint, uint32_t QPNum, uint32_t requestId);
    uint32_t sendHandshakeRequest(const EndPoint& endpoint, uint32_t QPNum);
    int handshakeId = 0;
    bool processUDSendQueue();
    bool processUDCQ();
    static void processCompletedSRs(std::array<temporary_buffer<uint8_t>, SendWRData::maxWR>& buffers, SendWRData& WRData, uint64_t signaledID);
    static constexpr int pollBatchSize = 16;

    static void fillAHAttr(struct ibv_ah_attr& AHAttr, const union ibv_gid& GID);
    std::unordered_map<union ibv_gid, struct ibv_ah*> AHLookup;
    std::unordered_map<int, weak_ptr<RDMAConnection>> handshakeLookup;
    static std::unique_ptr<RDMAStack> makeUDQP(std::unique_ptr<RDMAStack> stack);

    static constexpr int maxExpectedConnections = 1024;
    static constexpr int RCCQSize = RecvWRData::maxWR +
            (maxExpectedConnections * (SendWRData::maxWR / SendWRData::signalThreshold));
    RecvWRData RCQPRRs;
    struct ibv_cq* RCCQ = nullptr;
    struct ibv_srq* SRQ = nullptr;
    std::unordered_map<uint32_t, weak_ptr<RDMAConnection>> RCLookup;
    bool processRCCQ();
    int RCConnectionCount = 0;

    void registerPoller();

    bool acceptPromiseActive = false;
    promise<std::unique_ptr<RDMAConnection>> acceptPromise;
    std::deque<std::unique_ptr<RDMAConnection>> acceptQueue;

    //TODO move to slow core
    struct ibv_ah* makeAH(const union ibv_gid& GID);

    friend class RDMAConnection;
};

class RDMAListener {
public:
    RDMAListener():_rstack(0){}
    RDMAListener(RDMAStack* rstack):_rstack(rstack) {}
    ~RDMAListener() {}
    future<std::unique_ptr<RDMAConnection>> accept() {
        assert(_rstack);
        return _rstack->accept();
    }
    future<> close() {
        return make_ready_future<>();
    }
private:
    RDMAStack* _rstack;
}; // class RDMAListener

} // namespace rdma
} // namespace seastar
