// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransport.cc:
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

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/fasttransport.h"

#include "vr/vr-proto.pb.h"

#include <google/protobuf/message.h>
#include <event2/event.h>
#include <event2/thread.h>

#include <memory>
#include <random>
#include <string_view>
#include <charconv>

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <signal.h>
#include <thread>
#include <sched.h>

#include <numa.h>
#include <boost/fiber/all.hpp>

#include "rpc.h"

HashMap<uint8_t, std::string_view> index_to_protobuf_message;
std::vector<std::string_view> protobuf_message_indices;

static HashMap<TransportReceiver*, FastTransport*> unique_receivers;
static HashMap<UDPTransportAddress, FastTransport*> address_to_transports;

// #define USE_ALLOC_QUEUE
#ifdef USE_ALLOC_QUEUE
static MultiProducerSingleConsumerQueue<req_tag_t*> client_req_tag_queue;
#endif

static std::tuple<std::string_view, std::string_view> DecodePacket(const char *buf, size_t sz, void **meta_data)
{
    /* packet format:
     * type length + type + data length + data
     */

    std::string_view type;
    std::string_view msg;

    const char* ptr = buf;
    auto type_index = ptr[0];
    type = protobuf_message_indices[type_index];
    ptr += sizeof(uint8_t);

    size_t msgLen = *((size_t *)ptr);
    ptr += sizeof(size_t);
    ASSERT(ptr - buf < sz);

    if (ptr + msgLen - buf > sz)
    {
        Panic("Unable to decode packet out of size: %llu msgLen + %llu overhead (%llu) < %llu sz", msgLen, ptr - buf, ptr + msgLen - buf, sz);
    }

    msg = std::string_view(ptr, msgLen);
    ptr += msgLen;

    return {type, msg};
}

// A basic session management handler that expects successful responses
void basic_sm_handler(int session_num, erpc::SmEventType sm_event_type,
                      erpc::SmErrType sm_err_type, void *_context) {

    auto *c = static_cast<AppContext *>(_context);

    Assert(sm_err_type == erpc::SmErrType::kNoError);
    //  "SM response with error " + erpc::sm_err_type_str(sm_err_type));

    if (!(sm_event_type == erpc::SmEventType::kConnected ||
          sm_event_type == erpc::SmEventType::kDisconnected)) {
        throw std::runtime_error("Received unexpected SM event.");
    }

    Debug2("Rpc %u: Session number %d %s. Error %s. "
            "Time elapsed = %.3f s.\n",
            c->rpc->get_rpc_id(), session_num,
            erpc::sm_event_type_str(sm_event_type).c_str(),
            erpc::sm_err_type_str(sm_err_type).c_str(),
            c->rpc->sec_since_creation());
}

std::string UDPTransportAddressToString(const UDPTransportAddress& addr)
{
    char buf[INET_ADDRSTRLEN] = { 0 };
    const auto& sa = addr.GetSockAddress();
    inet_ntop(AF_INET, &(sa.sin_addr), buf, INET_ADDRSTRLEN);
    std::string str = std::string(buf) + ":" + std::to_string(sa.sin_port);
    return str;
}

UDPTransportAddress StringToUDPTransportAddress(std::string_view s)
{
    struct sockaddr_in sa;
    auto found_colon = s.find(":");
    auto ip_part = std::string(s.substr(0, found_colon));
    if (!inet_pton(AF_INET, ip_part.c_str(), &sa.sin_addr))
    {
        PPanic("Could not convert %s to UDPTransportAddress", s.data());
    }
    auto start_port_pos = s.data() + found_colon + 1;
    auto result = std::from_chars(start_port_pos, s.data() + s.size(), sa.sin_port);
    if (result.ec == std::errc::invalid_argument)
    {
        PPanic("Could not convert %s to UDPTransportAddress", s.data());
    }
    return UDPTransportAddress(sa);
}

static std::mutex fasttransport_lock;
static volatile bool fasttransport_initialized = false;

// Function called when we received a response to a
// request we sent on this transport
static void fasttransport_response(void *_context, void *_tag) {
    auto *c = static_cast<AppContext *>(_context);
    auto *rt = reinterpret_cast<req_tag_t *>(_tag);
    const UDPTransportAddress& sender_address = dynamic_cast<const UDPTransportAddress&>(rt->src->GetAddress());
    c->client.req_tag_pool.free(rt);

    void *meta_data = NULL;

    char* buf = reinterpret_cast<char *>(rt->resp_msgbuf.buf_);
    auto sz = rt->resp_msgbuf.get_data_size();

    auto [msg_type, msg] = DecodePacket(buf, sz, &meta_data);

    auto* src = rt->src;
    auto dst = rt->dst;
    //Notice("[DEBUG] Received response [%s] from %s to %s", msg_type.data(), UDPTransportAddressToString(sender_address).c_str(), UDPTransportAddressToString(dst).c_str());
    src->ReceiveMessage(dst, msg_type, msg, meta_data);

#ifdef USE_ALLOC_QUEUE
    if (rt->client_facing)
    {
        client_req_tag_queue.enqueue(rt);
    }
    else
    {
        c->client.req_tag_pool.free(rt);
    }
#else
    // c->client.req_tag_pool.free(rt);
#endif
}

// Function called when we received a request
static void fasttransport_request(erpc::ReqHandle *req_handle, void *_context) {
    Debug2("[%s] [sess id: %d] [RPC id: %d] [Server session num: %d] [Req num %d] Received fast transport request",
        req_handle->session_->get_remote_hostname().c_str(),
        req_handle->session_->local_session_num_,
        req_handle->get_server_rpc_id(),
        req_handle->get_server_session_num(),
        req_handle->get_cur_req_num());

    auto *c = static_cast<AppContext *>(_context);

    const auto& req_msgbuf = req_handle->get_req_msgbuf();

    char* buf = reinterpret_cast<char *>(req_msgbuf->buf_);
    auto sz = req_msgbuf->get_data_size();

    ASSERT(sizeof(uint32_t) - sz > 0);
    uint32_t magic = *(uint32_t*)buf;
    
    void *meta_data = NULL;
    auto [msg_type, msg] = DecodePacket(buf, sz, &meta_data);

    // Cache transport address, so it doesn't take as long to convert strings into udp transport address
    auto session_id = 0;
    auto& session = req_handle->session_;
    session_id = session->client_.session_num_;

    auto UpdateAddressMapping = [&]()
    {
        /*
        std::string hostname;
        if (session->is_client())
        {
            hostname = session->server_.uri();
        }
        else
        {
            hostname = session->client_.uri();
        }
        */
        auto hostname = session->client_.uri();

        auto& [existing_req_handle, transport_address] = c->server.session_id_to_request_handle_and_address[session_id];
        transport_address = StringToUDPTransportAddress(hostname);
        //Notice("[DEBUG] update address mapping %s with session %d", hostname.c_str(), session_id);
        c->server.address_to_session_id.emplace(transport_address, session_id);
    };

    // Check that the session id is valid
    if (auto found = c->server.session_to_address_index.find(session); found != std::end(c->server.session_to_address_index))
    {
        session_id = found->second;
    }
    else
    {
        std::string hostname;
        if (session->is_client())
        {
            hostname = session->server_.uri();
        }
        else
        {
            hostname = session->client_.uri();
        }
        
        auto match_transport_address = StringToUDPTransportAddress(hostname);

        // Search through sessions to find the corresponding session id
        bool found_session_id = false;
        for (auto i = 0; i < c->server.session_id_to_request_handle_and_address.size(); i++)
        {
            auto& [existing_req_handle, transport_address] = c->server.session_id_to_request_handle_and_address[i];
            //Notice("Searching for %i - %s - matching %s", i, hostname.c_str(), UDPTransportAddressToString(transport_address).c_str());
            if (transport_address == match_transport_address)
            {
                session_id = i;
                //Notice("Emplace session id %s -> %d", hostname.c_str(), session_id);
                c->server.session_to_address_index[session] = session_id;
                c->server.address_to_session_id[match_transport_address] = session_id;
                found_session_id = true;
                break;
            }
        }

        // otherwise just stick with client address
        // session_id = session->client_.session_num_;
        // happens only on the server
        if (!found_session_id)
        {
            c->server.session_to_address_index[session] = session_id;
            c->server.address_to_session_id[match_transport_address] = session_id;
        }
    }

    // If we get a connection that we haven't seen before, update all mappings
    if (c->server.session_id_to_request_handle_and_address.size() <= session_id)
    {
        c->server.session_id_to_request_handle_and_address.resize(session_id + 1);
        UpdateAddressMapping();
    }

    auto& [existing_req_handle, transport_address] = c->server.session_id_to_request_handle_and_address[session_id];

    // Check if empty
    if (transport_address == UDPTransportAddress{})
    {
        UpdateAddressMapping();
    }

    // if handle was updated, we need to update the relevant mappings
    //transport_address.session_id = session_id;
    existing_req_handle = req_handle;

    /*
    std::string hostname;
    if (session->is_client())
    {
        hostname = session->server_.uri();
    }
    else
    {
        hostname = session->client_.uri();
    }

    const UDPTransportAddress& sender_address = dynamic_cast<const UDPTransportAddress&>(c->server.receiver->GetAddress());
    Notice("[DEBUG] Received request [%s] from %s to %s HOST [%s] %s -> %s [%d -> %d] (local %d) (current %d)",
        msg_type.data(),
        UDPTransportAddressToString(sender_address).c_str(),
        UDPTransportAddressToString(transport_address).c_str(),
        hostname.c_str(),
        session->server_.uri().c_str(),
        session->client_.uri().c_str(),
        session->server_.session_num_,
        session->client_.session_num_,
        session->local_session_num_,
        session_id
        );
    */
    c->server.receiver->ReceiveMessage(transport_address, msg_type, msg, meta_data);
}

FastTransport::FastTransport(const specpaxos::Configuration &config,
                             int nthreads,
                             uint8_t nr_req_types,
                             uint8_t phy_port,
                             uint8_t numa_node,
                             uint8_t id,
                             uint64_t send_buf_size,
                             uint64_t receive_buf_size)
    : config(config),
      nr_req_types(nr_req_types),
      phy_port(phy_port),
      nthreads(nthreads),
      numa_node(numa_node),
      id(id),
      send_buf_size(send_buf_size),
      receive_buf_size(receive_buf_size)
    {

    Assert(numa_node <=  numa_max_node());

    c = new AppContext();

    // The first thread to grab the lock initializes the transport
    fasttransport_lock.lock();

    if (index_to_protobuf_message.empty())
    {
        using namespace specpaxos;
        using namespace specpaxos::vr::proto;
        std::vector<const Message*> messages;
        messages.emplace_back(&MsgLogEntry::default_instance());
        messages.emplace_back(&Request::default_instance());
        messages.emplace_back(&ShardOp::default_instance());
        messages.emplace_back(&UnloggedRequest::default_instance());
        messages.emplace_back(&RequestMessage::default_instance());
        messages.emplace_back(&ReplyMessage::default_instance());
        messages.emplace_back(&UnloggedRequestMessage::default_instance());
        messages.emplace_back(&UnloggedReplyMessage::default_instance());
        messages.emplace_back(&PrepareMessage::default_instance());
        messages.emplace_back(&PrepareOKMessage::default_instance());
        messages.emplace_back(&CommitMessage::default_instance());
        messages.emplace_back(&CommitOKMessage::default_instance());
        messages.emplace_back(&RequestStateTransferMessage::default_instance());
        messages.emplace_back(&StateTransferMessage::default_instance());
        messages.emplace_back(&StartViewChangeMessage::default_instance());
        messages.emplace_back(&DoViewChangeMessage::default_instance());
        messages.emplace_back(&StartViewMessage::default_instance());
        messages.emplace_back(&RecoveryMessage::default_instance());
        messages.emplace_back(&RecoveryResponseMessage::default_instance());
        messages.emplace_back(&MakeDurabilityServerKnownMessage::default_instance());
        messages.emplace_back(&StatusUpdateMessage::default_instance());
        
        for (auto i = 0; i < messages.size(); i++)
        {
            auto& message = messages[i];
            auto& name = message->GetDescriptor()->full_name();
            auto index = message->GetDescriptor()->index();
            index_to_protobuf_message[index] = name;
            if (protobuf_message_indices.size() <= index)
            {
                protobuf_message_indices.resize(index + 1);
            } 
            protobuf_message_indices[index] = name;
        }
    }

    if (fasttransport_initialized) {
        // Create the event_base to schedule requests
        eventBase = event_base_new();
        evthread_make_base_notifiable(eventBase);
    } else {
        // Setup libevent
        evthread_use_pthreads(); // TODO: do we really need this even
                                 // when we manipulate one eventbase
                                 // per thread?
        event_set_log_callback(LogCallback);
        event_set_fatal_callback(FatalCallback);

        // Create the event_base to schedule requests
        eventBase = event_base_new();
        evthread_make_base_notifiable(eventBase);

        // signals must be registered only on one eventBase
        // signalEvents.push_back(evsignal_new(eventBase, SIGTERM,
        //         SignalCallback, this));
        // signalEvents.push_back(evsignal_new(eventBase, SIGINT,
        //         SignalCallback, this));

        for (event *x : signalEvents) {
            event_add(x, NULL);
        }

        fasttransport_initialized = true;
    }
    
    fasttransport_lock.unlock();
}

FastTransport::~FastTransport() {
}

std::string FastTransport::GetLocalAddress() {
    struct ifaddrs *addrs;
    getifaddrs(&addrs);
    std::string ip_address;
    for (struct ifaddrs *iap = addrs; iap != NULL; iap = iap->ifa_next) {
        // Consider only active IPv4 iterfaces
        if (iap->ifa_addr && (iap->ifa_flags & IFF_UP) &&
            iap->ifa_addr->sa_family == AF_INET) {
            auto *sa = reinterpret_cast<struct sockaddr_in *>(iap->ifa_addr);

            char ip_addr[32];
            inet_ntop(iap->ifa_addr->sa_family, &(sa->sin_addr), ip_addr,
                        sizeof(ip_addr));

            // grab the first one
            auto ip_addr_view = std::string_view(ip_addr);
            Warning("%s", ip_addr_view.data());
            if (ip_addr_view.substr(0, 3) == "10.") {
                ip_address = std::string(ip_addr_view);
                break;
            }
            if (ip_addr_view.substr(0, 3) == "127") {
                continue;
            }
            ip_address = std::string(ip_addr_view);
            break;
        }
    }
    freeifaddrs(addrs);
    if (ip_address.empty())
    {
        Panic("no local ip found");
    }
    return ip_address;
}

void FastTransport::Register(TransportReceiver *receiver,
                       const specpaxos::Configuration &config,
                       int groupIdx,
                       int replicaIdx)
{
    // We are registering a internal messaging system
    unique_receivers.emplace(receiver, this);

    if (groupIdx == -2 && replicaIdx == -2)
    {
        const UDPTransportAddress& sender_address = dynamic_cast<const UDPTransportAddress&>(c->server.receiver->GetAddress());
        //Notice("[DEBUG] GOT ADDRESS %s -> %s", UDPTransportAddressToString(sender_address).c_str(), UDPTransportAddressToString(sender_address).c_str());

        const specpaxos::Configuration *canonicalConfig = RegisterConfiguration(receiver, config, -1, -1);

        return;
    }

    {
        std::lock_guard<std::mutex> l(fasttransport_lock);
        ASSERT(replicaIdx < config.n);

        // Setup eRPC
        std::string ip;
        int port;
        if (groupIdx == -1)
        {
            // Always bind local address to "xxx.xxx.xxx.xxx:31850"
            ip = GetLocalAddress();
            static int increasing_port = 31850;
            port = increasing_port++;
        }
        else
        {
            // Otherwise, we're on a server...
            auto replica_config = config.replica(groupIdx, replicaIdx);
            ip = replica_config.host;
            port = std::stoi(replica_config.port);// + id;
        }
        local_ip = ip;
        local_port = std::to_string(port);
        local_uri = local_ip + ":" + local_port;

        if (!nexus) {
            static HashMap<std::string, erpc::Nexus*> ip_to_nexus;
            if (auto found = ip_to_nexus.find(local_uri); found != std::end(ip_to_nexus)) {
                nexus = found->second;
            } else {
                nexus = new erpc::Nexus(local_uri, numa_node, 0);
                Warning("Created nexus object with local_uri = %s", local_uri.c_str());

                // register receive handlers
                for (uint8_t j = 0; j <= nr_req_types; j++) {
                    nexus->register_req_func(j, fasttransport_request, erpc::ReqFuncType::kForeground);
                }

                ip_to_nexus.emplace(local_uri, nexus);
            }

            // Create the RPC object
            c->rpc = new erpc::Rpc<erpc::CTransport>(nexus,
                                                    static_cast<void *>(c),
                                                    static_cast<uint8_t>(id),
                                                    basic_sm_handler, phy_port);
            c->rpc->retry_connect_on_invalid_rpc_id_ = true;
        }

        sockaddr_in sock_addr{};
        inet_pton(AF_INET, ip.c_str(), &(sock_addr.sin_addr));
        sock_addr.sin_port = port;

        UDPTransportAddress *addr = new UDPTransportAddress(sock_addr);
        receiver->SetAddress(addr);
        address_to_transports.emplace(*addr, this);
    }

    const specpaxos::Configuration *canonicalConfig =
        RegisterConfiguration(receiver, config, groupIdx, replicaIdx);
        
    auto MapUDPAddressToReceiver = [&](auto replica_uri, auto session_id)
    {
        // Construct udp address
        sockaddr_in sock_addr{};

        inet_pton(AF_INET, replica_uri.host.c_str(), &(sock_addr.sin_addr));
        sock_addr.sin_port = std::stoi(replica_uri.port);

        UDPTransportAddress addr = UDPTransportAddress(sock_addr);

        c->server.address_to_session_id[addr] = session_id;

        if (c->server.session_id_to_request_handle_and_address.size() <= session_id)
        {
            c->server.session_id_to_request_handle_and_address.resize(session_id + 1);
        }

        c->server.session_id_to_request_handle_and_address[session_id] = RequestHandleAndAddress{ nullptr, addr };
    };

    this->replicaIdx = replicaIdx;
    c->server.receiver = receiver;
    for (auto i = 0; i < config.n; i++)
    {
        auto replica_uri = config.replica(0, i);
        auto session_id = GetSession(receiver, i, 0);
        if (session_id != -1)
        {
            MapUDPAddressToReceiver(replica_uri, session_id);

            while (!c->rpc->is_connected(session_id)) {
                c->rpc->run_event_loop_once();
            }
        }
    }
}

inline req_tag_t* FastTransport::GetReqTag(size_t reqLen, size_t respLen) {
    // create a new request tag
    if (reqLen == 0)
        reqLen = c->rpc->get_max_data_per_pkt();
    if (respLen == 0)
        respLen = c->rpc->get_max_data_per_pkt();

#ifdef USE_ALLOC_QUEUE
    req_tag_t* crt_req_tag;
    if (reqLen == 0 && respLen == 0)
    {
        if (!client_req_tag_queue.try_dequeue(crt_req_tag))
        {
            crt_req_tag = new req_tag_t();
            crt_req_tag->client_facing = true;
        }
    }
    else
    {
        crt_req_tag = c->client.req_tag_pool.alloc();
    }
#else
    auto crt_req_tag = c->client.req_tag_pool.alloc();
#endif

    // if (id == 0)
    // {
    //     static std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    //     if (now > std::chrono::system_clock::now() + std::chrono::seconds(1))
    //     {
    //         now = std::chrono::system_clock::now();
    //         Warning("[%d] Alloc pool size = %d [%d]", id, c->client.req_tag_pool.alloc_size, c->client.req_tag_pool.total_size);
    //     }
    // }

    if (!crt_req_tag->initialized)
    {
        crt_req_tag->initialized = true;
        crt_req_tag->req_msgbuf = c->rpc->alloc_msg_buffer_or_die(reqLen);
        crt_req_tag->resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(respLen);
    }
    return crt_req_tag;
}

inline int FastTransport::GetSession(TransportReceiver *src, uint8_t replicaIdx, uint8_t dstRpcIdx) {
    // TODO: Figure out group index
    const auto groupIdx = dstRpcIdx;

    auto session_key = std::make_pair(replicaIdx, dstRpcIdx);

    auto& session = c->client.sessions[src];
    const auto iter = session.find(session_key);
    if (iter == session.end()) {
        // create a new session to the replica core
        // use the default port from eRPC for control path
        auto replica_uri = config.replica(groupIdx, replicaIdx);
        if (replica_uri.host == local_ip) {
            c->replica_index = 0;
            return -1;
        }
        auto uri = replica_uri.host + ":" + replica_uri.port;        
        int session_id = c->rpc->create_session(uri, id);
        /*
        while (!c->rpc->is_connected(session_id)) {
            c->rpc->run_event_loop_once();
        }
        */
        session[session_key] = session_id;
        Warning("Opened eRPC session to %s, RPC id: %d, session_id %d", uri.c_str(), id, session_id);
        return session_id;
    } else {
        return iter->second;
    }
}

void FastTransport::Run()
{
    while (!stop)
    {
        event_base_loop(eventBase, EVLOOP_ONCE|EVLOOP_NONBLOCK);
        if (on_event_loop_function)
        {
            on_event_loop_function();
        }
        c->rpc->run_event_loop_once();
        
        if (queue_has_message)
        {
            while (true)
            {
                ProtobufMessage protobuf_message;
                if (internal_message_queue.try_dequeue(protobuf_message))
                {
                    queue_has_message = false;
                    auto& [transport_address, index, msg] = protobuf_message;
                    //Notice("[DEBUG] Receiving message from %s to %s", UDPTransportAddressToString(transport_address).c_str(), UDPTransportAddressToString(transport_address).c_str());
                    std::string_view msg_type = protobuf_message_indices[index];
                    c->server.receiver->ReceiveMessage(transport_address, msg_type, msg, nullptr);
                }
                else
                {
                    break;
                }
            }
        }
    }
}

void FastTransport::RunOnce()
{
    event_base_loop(eventBase, EVLOOP_ONCE|EVLOOP_NONBLOCK);
    if (on_event_loop_function)
    {
        on_event_loop_function();
    }
    c->rpc->run_event_loop_once();
    
    if (queue_has_message)
    {
        while (true)
        {
            ProtobufMessage protobuf_message;
            if (internal_message_queue.try_dequeue(protobuf_message))
            {
                queue_has_message = false;
                auto& [transport_address, index, msg] = protobuf_message;
                //Notice("[DEBUG] Receiving message from %s to %s", UDPTransportAddressToString(transport_address).c_str(), UDPTransportAddressToString(transport_address).c_str());
                std::string_view msg_type = protobuf_message_indices[index];
                c->server.receiver->ReceiveMessage(transport_address, msg_type, msg, nullptr);
            }
            else
            {
                break;
            }
        }
    }
}

int
FastTransport::Timer(uint64_t ms, timer_callback_t cb)
{
    auto& [info, index] = timers.Allocate();
    if (!info)
    {
        info = new FastTransportTimerInfo();
    }

    struct timeval tv;
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;

    info->transport = this;
    info->id = index;
    info->cb = cb;
    info->ev = event_new(eventBase, -1, 0,
                        TimerCallback, info);
    info->tv = tv;

    event_add(info->ev, &tv);

    return info->id;
}

int
FastTransport::TimerUS(uint64_t us, timer_callback_t cb)
{
    auto& [info, index] = timers.Allocate();
    if (!info)
    {
        info = new FastTransportTimerInfo();
    }

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = us;

    info->transport = this;
    info->id = index;
    info->cb = cb;
    info->ev = event_new(eventBase, -1, 0,
                        TimerCallback, info);
    info->tv = tv;

    event_add(info->ev, &tv);

    return info->id;
}

int
FastTransport::ManualEvent(timer_callback_t cb) {
    auto& [info, index] = manualevents.Allocate();
    if (!info)
    {
        info = new FastTransportTimerInfo();
    }

    info->transport = this;
    info->id = index;
    info->cb = cb;
    info->ev = event_new(eventBase, -1, EV_PERSIST | EV_READ,
                        ManualCallback, info);

    event_add(info->ev, NULL);

    return info->id;
}

bool
FastTransport::CancelTimer(int id)
{
    auto& info = timers.Get(id);

    event_del(info->ev);
    event_free(info->ev);

    timers.Free(info->id);

    return true;
}

void
FastTransport::CancelAllTimers()
{
    Panic("Unsupported");
}

void
FastTransport::OnTimer(FastTransportTimerInfo *info)
{
    event_del(info->ev);
    event_free(info->ev);

    info->cb();

    timers.Free(info->id);
}

bool
FastTransport::TriggerManualEvent(int id){
    auto& info = manualevents.Get(id);
    event_active(info->ev, EV_WRITE, 0);
    return true;
}

void
FastTransport::OnManualEvent(FastTransportTimerInfo *info)
{
    info->cb();
}

void FastTransport::TimerCallback(evutil_socket_t fd, short what, void *arg) {
    FastTransport::FastTransportTimerInfo *info =
        (FastTransport::FastTransportTimerInfo *)arg;

    ASSERT(what & EV_TIMEOUT);

    info->transport->OnTimer(info);
}

void
FastTransport::ManualCallback(evutil_socket_t fd, short what, void *arg)
{
    FastTransport::FastTransportTimerInfo *info =
        (FastTransport::FastTransportTimerInfo *)arg;
    ASSERT(what & EV_WRITE);
    info->transport->OnManualEvent(info);
}

void FastTransport::LogCallback(int severity, const char *msg) {
    Message_Type msgType;
    switch (severity) {
    case _EVENT_LOG_DEBUG:
        msgType = MSG_DEBUG;
        break;
    case _EVENT_LOG_MSG:
        msgType = MSG_NOTICE;
        break;
    case _EVENT_LOG_WARN:
        msgType = MSG_WARNING;
        break;
    case _EVENT_LOG_ERR:
        msgType = MSG_WARNING;
        break;
    default:
        NOT_REACHABLE();
    }

    _Message(msgType, "libevent", 0, NULL, "%s", msg);
}

void FastTransport::FatalCallback(int err) {
    Panic("Fatal libevent error: %d", err);
}

void FastTransport::SignalCallback(evutil_socket_t fd,
      short what, void *arg) {
    Notice("Terminating on SIGTERM/SIGINT");
    FastTransport *transport = (FastTransport *)arg;
    //event_base_loopbreak(libeventBase);
    transport->Stop();
}

void FastTransport::Stop() {
    Debug2("Stopping transport!");
    stop = true;
}

std::size_t GetSerializeMessageLen(const google::protobuf::Message& m, size_t data_len, size_t meta_len, void *meta_data)
{
    /* packet format:
     * type length + type + data length + data
     */
    size_t typeLen = sizeof(uint8_t);
    size_t dataLen = data_len;
    size_t totalLen = typeLen + sizeof(data_len) + data_len;
    return totalLen;
}

static size_t
SerializeMessage(const google::protobuf::Message& m, size_t data_len,
                 char *buf, size_t meta_len, void *meta_data)
{
    /* packet format:
     * FRAG_MAGIC + meta_len + meta + type length + type + data length + data
     */
    char* ptr = buf;
    
    size_t typeLen = sizeof(uint8_t);
    size_t dataLen = data_len;
    size_t totalLen = typeLen + sizeof(dataLen) + dataLen;

    auto protobuf_index = m.GetDescriptor()->index();
    *((uint8_t*) ptr) = protobuf_index;
    ptr += sizeof(uint8_t);

    *((size_t *) ptr) = dataLen;
    ptr += sizeof(size_t);
    ASSERT(ptr - buf < totalLen);
    ASSERT(ptr + dataLen - buf == totalLen);
    m.SerializeWithCachedSizesToArray(reinterpret_cast<uint8_t*>(ptr));
    ptr += dataLen;

    return totalLen;
}

bool FastTransport::SendMessageInternal(TransportReceiver *src,
                             const UDPTransportAddress &dst,
                             const Message &m)
{
    const auto& dst_addr = dst.GetSockAddress();
    const auto meta_len = 0;
    const auto meta_data = nullptr;

    // Try to get the cached size
    auto data_len = m.GetCachedSize();
    if (data_len == 0)
    {
        // Otherwise compute total size
        data_len = m.ByteSizeLong();
    }
    auto message_length = GetSerializeMessageLen(m, data_len, meta_len, meta_data);

    /*
    const UDPTransportAddress& sender_address = dynamic_cast<const UDPTransportAddress&>(src->GetAddress());
    Notice("[DEBUG] SendMessageInternal [%s] from %s to %s", m.GetTypeName().c_str(), UDPTransportAddressToString(sender_address).c_str(), UDPTransportAddressToString(dst).c_str());
    */

    if (auto found = address_to_transports.find(dst); found != std::end(address_to_transports))
    {
        auto& transport = found->second;
        auto index = m.GetDescriptor()->index();
        auto data = m.SerializeAsString();
        const UDPTransportAddress& sender_address = dynamic_cast<const UDPTransportAddress&>(src->GetAddress());
        const UDPTransportAddress& my_address = dynamic_cast<const UDPTransportAddress&>(c->server.receiver->GetAddress());
        ProtobufMessage protobuf_message{ sender_address, index, data };
        //Notice("[DEBUG] Sending message from %s - %s to %s", UDPTransportAddressToString(my_address).c_str(), UDPTransportAddressToString(sender_address).c_str(), UDPTransportAddressToString(dst).c_str());
        while (!transport->internal_message_queue.try_enqueue(protobuf_message));
        transport->queue_has_message = true;
        return true;
    }

    // Actually edit transport address... cast away const
    auto& session_id = const_cast<UDPTransportAddress&>(dst).session_id;
    if (session_id == -1)
    {
        // Perform lookup to cache the session id into transport address
        if (auto found = c->server.address_to_session_id.find(dst); found != std::end(c->server.address_to_session_id))
        {
            session_id = found->second;
        }
        else
        {
            Panic("Could not find session id associated with dst address %s", UDPTransportAddressToString(dst).c_str());
        }
    }
    auto& [req_handle, transport_address] = c->server.session_id_to_request_handle_and_address[session_id];
    if (dst != transport_address)
    {
        Panic("Transport addresses not matching!");
    }

    auto SendRequest = [&]()
    {
        auto req_tag = GetReqTag(send_buf_size, receive_buf_size);

        auto &resp = req_tag->req_msgbuf;

        auto data_len_serialized = SerializeMessage(m, data_len, reinterpret_cast<char*>(resp.buf_), meta_len, meta_data);
        if (data_len_serialized > resp.max_data_size_)
        {
            Panic("SendRequest: Failed to serialize data into data buffer %llu > %llu", data_len_serialized, resp.max_data_size_);
        }
        erpc::Rpc<erpc::CTransport>::resize_msg_buffer(&resp, message_length);

        auto req_type = 0;
        //Notice("[DEBUG] Sending request [%s] from %s to %s [%d]", m.GetTypeName().c_str(), UDPTransportAddressToString(sender_address).c_str(), UDPTransportAddressToString(dst).c_str(), session_id);

        req_tag->src = src;
        req_tag->dst = dst;
        req_tag->req_type = req_type;
        c->rpc->enqueue_request(session_id, req_type,
                                &req_tag->req_msgbuf,
                                &req_tag->resp_msgbuf,
                                fasttransport_response,
                                reinterpret_cast<void *>(req_tag));
    };

    auto SendResponse = [&]()
    {
        //Notice("[DEBUG] Sending response [%s] from %s to %s [%d]", m.GetTypeName().c_str(), UDPTransportAddressToString(sender_address).c_str(), UDPTransportAddressToString(dst).c_str(), session_id);
        erpc::MsgBuffer &resp = req_handle->dyn_resp_msgbuf_;
        resp = c->rpc->alloc_msg_buffer_or_die(message_length);
        //erpc::Rpc<erpc::CTransport>::resize_msg_buffer(
        //    &req_handle->pre_resp_msgbuf_, message_length);
        //erpc::MsgBuffer &resp = req_handle->pre_resp_msgbuf_;
        
        auto data_len_serialized = SerializeMessage(m, data_len, reinterpret_cast<char*>(resp.buf_), meta_len, meta_data);
        if (data_len_serialized > resp.get_data_size())
        {
            Panic("SendResponse: Failed to serialize data into data buffer %llu > %llu", data_len_serialized, resp.get_data_size());
        }

        c->rpc->enqueue_response(req_handle, &resp);

        req_handle = nullptr;
    };

    if (req_handle)
    {
        SendResponse();
    }
    else
    {
        SendRequest();
    }

    return true;
}

UDPTransportAddress FastTransport::LookupAddress(const specpaxos::ReplicaAddress &addr)
{
    struct sockaddr_in sa{};
    inet_pton(AF_INET, addr.host.c_str(), &(sa.sin_addr));
    sa.sin_port = std::stoi(addr.port);
    UDPTransportAddress out = UDPTransportAddress(sa);
    return out;
}

UDPTransportAddress FastTransport::LookupAddress(const specpaxos::Configuration &config,
                            int groupIdx,
                            int replicaIdx)
{
    const specpaxos::ReplicaAddress &addr = config.replica(groupIdx,
                                                           replicaIdx);
    return LookupAddress(addr);
}

const UDPTransportAddress* FastTransport::LookupMulticastAddress(const specpaxos::Configuration *config)
{
    if (!config->multicast()) {
        // Configuration has no multicast address
        return NULL;
    }

    /*
    if (multicastFds.find(config) != multicastFds.end()) {
        // We are listening on this multicast address. Some
        // implementations of MOM aren't OK with us both sending to
        // and receiving from the same address, so don't look up the
        // address.
        return NULL;
    }
    */

    UDPTransportAddress *addr =
        new UDPTransportAddress(LookupAddress(*(config->multicast())));
    return addr;
}

const UDPTransportAddress* FastTransport::LookupFCAddress(const specpaxos::Configuration* config)
{
    if (!config->fc()) {
        // Configuration has no failure coorinator address
        return NULL;
    }
    UDPTransportAddress *addr =
        new UDPTransportAddress(LookupAddress(*(config->fc())));
    return addr;
}
