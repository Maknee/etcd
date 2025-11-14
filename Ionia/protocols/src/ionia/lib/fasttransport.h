// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransport.h:
 *   message-passing network interface that uses UDP message delivery
 *   and libasync
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *           2018 Adriana Szekeres  <aaasz@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#ifndef _LIB_FASTTRANSPORT_H_
#define _LIB_FASTTRANSPORT_H_

#include "lib/configuration.h"
#include "lib/transport.h"
#include "lib/message.h"
#include "lib/udptransport.h"

#include "rpc.h"
#include "rpc_constants.h"

#include "util/numautils.h"
#include <gflags/gflags.h>

#include "lib/data_structure.h"

#include <event2/event.h>

#include <map>
#include <list>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <random>
#include <mutex>
#include <atomic>
#include <optional>
#include <netinet/in.h>

#include "parallel_hashmap/phmap.h"
#include "third_party/concurrentqueue/concurrentqueue.h"
#include "third_party/readerwriterqueue/readerwriterqueue.h"
#include <folly/FBVector.h>

/*
 * Class FastTransport implements a multi-threaded
 * transport layer based on eRPC which works with
 * a client - server configuration, where the server
 * may have multiple replicas.
 *
 * The Register function is used to register a transport
 * receiver. The transport is responsible for sending and
 * dispatching messages from/to its receivers accordingly.
 * A transport receiver can either be a client or a server
 * replica. A transport instance's receivers must be
 * of the same type.
 */

#define REQ_TAG_BUF_SIZE 0
#define REQ_TAG_POOL_LIMIT 1024

// A tag attached to every request we send;
// it is passed to the response function
struct req_tag_t {
    req_tag_t() = default;
    bool initialized = false;
    erpc::MsgBuffer req_msgbuf{};
    erpc::MsgBuffer resp_msgbuf{};
    uint8_t req_type{};
    TransportReceiver *src{};
    UDPTransportAddress dst;
    std::size_t index{};
    bool client_facing = false;
};

// A basic mempool for preallocated objects of type T. eRPC has a faster,
// hugepage-backed one.
template <class T> class AppMemPool {
    public:
        size_t num_to_alloc = 1;
        std::vector<T *> backing_ptr_vec;
        std::vector<T *> pool;

        // stats
        std::size_t alloc_size;
        std::size_t total_size;

    void extend_pool() {
        T *backing_ptr = new T[num_to_alloc];
        for (size_t i = 0; i < num_to_alloc; i++) pool.push_back(&backing_ptr[i]);
        backing_ptr_vec.push_back(backing_ptr);
        alloc_size = num_to_alloc;
        total_size += num_to_alloc;
        num_to_alloc *= 2;
    }

    T *alloc() {
        if (pool.empty()) extend_pool();
        T *ret = pool.back();
        pool.pop_back();
        alloc_size--;
        return ret;
    }

    void free(T *t) { pool.push_back(t); alloc_size++; }

    AppMemPool() {}
    ~AppMemPool() {
        for (T *ptr : backing_ptr_vec) delete[] ptr;
    }
};

struct ERPCSession {
    erpc::Nexus* nexus{};
    erpc::Rpc<erpc::CTransport>* rpc{};
    int session_id;
};

struct RequestHandleAndAddress
{
    erpc::ReqHandle* handle;
    UDPTransportAddress address;
};

// eRPC context passed between request and responses
class AppContext {
    public:
        struct {
            // Request tags used for RPCs exchanged with the servers
            AppMemPool<req_tag_t> req_tag_pool;
            HashMap<TransportReceiver*, std::map<std::pair<uint8_t, uint8_t>, int>> sessions;
        } client;

        struct {
            // Any connected address to its corresponding request handle
            std::vector<RequestHandleAndAddress> session_id_to_request_handle_and_address;

            // Lookup once for mapping from udp address to session id
            HashMap<UDPTransportAddress, int> address_to_session_id;

            // Cached mapping from request handle to transport address
            // updated in fasttransport_request only if it's a newly discovered client
            HashMap<erpc::Session*, int> session_to_address_index;
            

            std::vector<long> latency_get;
            std::vector<long> latency_prepare;
            std::vector<long> latency_commit;
            TransportReceiver *receiver = nullptr;
        } server;

        // common to both servers and clients
        erpc::Rpc<erpc::CTransport> *rpc = nullptr;

        // replica index
        int replica_index = -1;
};

class FastTransport : public TransportCommon<UDPTransportAddress>
{
public:
    FastTransport(const specpaxos::Configuration &config,
                  int nthreads,
                  uint8_t nr_req_types,
                  uint8_t phy_port,
                  uint8_t numa_node,
                  uint8_t id,
                  uint64_t send_buf_size = 0,
                  uint64_t receive_buf_size = 0);
    virtual ~FastTransport();
    virtual void Register(TransportReceiver *receiver,
                          const specpaxos::Configuration &config,
                          int groupIdx,
                          int replicaIdx) override;
    void Run() override;
    void RunOnce() override;
    void Wait();
    void Stop() override;
    int Timer(uint64_t ms, timer_callback_t cb) override;
    int TimerUS(uint64_t us, timer_callback_t cb) override;
    bool CancelTimer(int id) override;
    void CancelAllTimers() override;

    req_tag_t* GetReqTag(size_t reqLen, size_t respLen);
    int GetSession(TransportReceiver *src, uint8_t replicaIdx, uint8_t dstRpcIdx);

    //uint8_t GetID() override { return id; };

    /////////////////////////////
    int ManualEvent(timer_callback_t cb);
    bool TriggerManualEvent(int id);
    static std::string GetLocalAddress();

    struct ProtobufMessage
    {
        UDPTransportAddress address{};
        uint8_t index = 0;
        std::string data;
    };

    bool queue_has_message = false;
    MultiProducerSingleConsumerQueue<ProtobufMessage> internal_message_queue{1*1000};

private:
    // Configuration of the replicas
    specpaxos::Configuration config;

    // Number of request types
    uint8_t nr_req_types;

    // The port of the fast NIC
    uint8_t phy_port;

    // Number of server threads
    int nthreads;

    // numa node on which this transport thread is running
    uint8_t numa_node;

    // used as the RPC id, must be unique per transport thread
    uint8_t id;

    // Index of the replica server
    int replicaIdx;

    // Nexus object
    erpc::Nexus *nexus{};
    
    uint64_t send_buf_size;
    uint64_t receive_buf_size;

    // ip
    std::string local_ip;
    std::string local_port;
    std::string local_uri;

    struct FastTransportTimerInfo
    {
        FastTransport *transport;
        timer_callback_t cb;
        event *ev = nullptr;
        int id;
        struct timeval tv;
    };

    event_base *eventBase;
    std::vector<event *> signalEvents;
    AppContext *c;
    bool stop = false;

    ResizableIndexVector<FastTransportTimerInfo*> timers;
    ResizableIndexVector<FastTransportTimerInfo*> manualevents;

    void OnTimer(FastTransportTimerInfo *info);
    void OnManualEvent(FastTransportTimerInfo *info);
    static void SocketCallback(evutil_socket_t fd, short what, void *arg);
    static void TimerCallback(evutil_socket_t fd, short what, void *arg);
    static void ManualCallback(evutil_socket_t fd, short what, void *arg);
    static void LogCallback(int severity, const char *msg);
    static void FatalCallback(int err);
    static void SignalCallback(evutil_socket_t fd, short what, void *arg);

    /////////////////////////////
    bool SendMessageInternal(TransportReceiver *src,
                             const UDPTransportAddress &dst,
                             const Message &m) override;

    UDPTransportAddress LookupAddress(const specpaxos::ReplicaAddress &addr);
    UDPTransportAddress LookupAddress(const specpaxos::Configuration &cfg, int groupIdx, int replicaIdx) override;
    const UDPTransportAddress* LookupMulticastAddress(const specpaxos::Configuration *cfg) override;
    const UDPTransportAddress* LookupFCAddress(const specpaxos::Configuration *cfg) override;
};

void basic_sm_handler(int session_num, erpc::SmEventType sm_event_type,
                      erpc::SmErrType sm_err_type, void *_context);

#endif  // _LIB_FASTTRANSPORT_H_
