// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransport.h:
 *   message-passing network interface that uses UDP message delivery
 *   and libasync
 * 
 * Copyright 2021 Aishwarya Ganesan and Ramnatthan Alagappan
 *
 * Small changes made to the code to implement Skyros
 *
 * *************************************************************
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef _LIB_UDPTRANSPORT_H_
#define _LIB_UDPTRANSPORT_H_

#include "lib/configuration.h"
#include "lib/transport.h"
#include "lib/transportcommon.h"
#include "lib/workertasks.h"

#include <event2/event.h>

#include <map>
#include <list>
#include <vector>
#include <unordered_map>
#include <random>
#include <netinet/in.h>
#include <map>

class UDPTransportAddress : public TransportAddress
{
public:
    UDPTransportAddress * clone() const;
public:
    explicit UDPTransportAddress() = default;
    UDPTransportAddress(const sockaddr_in &addr);
    const auto& GetSockAddress() const { return addr; }
private:
    sockaddr_in addr;
    friend class UDPTransport;
    friend bool operator==(const UDPTransportAddress &a,
                           const UDPTransportAddress &b);
    friend bool operator!=(const UDPTransportAddress &a,
                           const UDPTransportAddress &b);
    friend bool operator<(const UDPTransportAddress &a,
                          const UDPTransportAddress &b);

public:
    // used only by fast transport
    int session_id = -1;
};

namespace std {
  template <> struct hash<UDPTransportAddress>
  {
    size_t operator()(const UDPTransportAddress& x) const
    {
        const auto& s = x.GetSockAddress();
        return (s.sin_port << sizeof(s.sin_addr.s_addr) * 8) + (s.sin_addr.s_addr);
    }
  };
}

template<typename T>
struct ResizableIndexVector
{
    struct IndexedT
    {
        T data;
        std::size_t index;
    };
    std::vector<IndexedT> t_datas{};
    std::vector<int> free_indices{};
    std::size_t free_indices_end = 0;

    IndexedT& Allocate()
    {
        // Check if free first
        if (free_indices_end != 0)
        {
            free_indices_end--;
            auto& free_index = free_indices[free_indices_end];
            auto& t = t_datas[free_index];
            //Notice("Allocate free %d %d", t.index, free_indices_end);
            return t;
        }

        // Otherwise allocate
        std::size_t index = t_datas.size();
        IndexedT t_data = IndexedT{T{}, index};
        t_datas.emplace_back(t_data);
        return t_datas[index];
    }

    void Free(std::size_t index)
    {
        if (free_indices.size() <= free_indices_end)
        {
            free_indices.resize(free_indices_end + 1);
        }
        free_indices[free_indices_end] = index;
        free_indices_end++;
        //Notice("Add free %d %d", free_indices[free_indices_end - 1], free_indices_end);
    }

    T& Get(std::size_t index)
    {
        if (index >= t_datas.size())
        {
            Panic("Getting out of bounds %llu > %llu", index, t_datas.size());
        }
        return t_datas[index].data;
    }
};

class UDPTransport : public TransportCommon<UDPTransportAddress>
{
public:
    UDPTransport(double dropRate = 0.0, double reorderRate = 0.0,
                 int dscp = 0, event_base *evbase = nullptr);
    virtual ~UDPTransport();
    virtual void Register(TransportReceiver *receiver,
                          const specpaxos::Configuration &config,
                          int groupIdx,
                          int replicaIdx) override;
    virtual bool OrderedMulticast(TransportReceiver *src,
                                  const std::vector<int> &groups,
                                  const Message &m) override;
    void Run() override;
    void Stop() override;
    int Timer(uint64_t ms, timer_callback_t cb) override;
    int TimerUS(uint64_t us, timer_callback_t cb) override;
    bool CancelTimer(int id) override;
    void CancelAllTimers() override;
    int ManualEvent(timer_callback_t cb) override;
    bool TriggerManualEvent(int id) override;


private:
    struct UDPTransportTimerInfo
    {
        UDPTransport *transport;
        timer_callback_t cb;
        event *ev;
        int id;
        struct timeval tv;
    };

    double dropRate;
    double reorderRate;
    std::uniform_real_distribution<double> uniformDist;
    std::default_random_engine randomEngine;
    struct
    {
        bool valid;
        UDPTransportAddress *addr;
        string msgType;
        string message;
        int fd;
    } reorderBuffer;
    int dscp;

    event_base *libeventBase;
    std::vector<event *> listenerEvents;
    std::vector<event *> signalEvents;
    std::map<int, TransportReceiver*> receivers; // fd -> receiver
    std::map<TransportReceiver*, int> fds; // receiver -> fd
    std::map<const specpaxos::Configuration *, int> multicastFds;
    std::map<int, const specpaxos::Configuration *> multicastConfigs;
    int lastTimerId;
    int lastMaunalEventId;
    //std::map<int, UDPTransportTimerInfo *> timers;
    //std::map<int, UDPTransportTimerInfo *> manualevents;
    ResizableIndexVector<UDPTransportTimerInfo*> timers;
    ResizableIndexVector<UDPTransportTimerInfo*> manualevents;
    uint64_t lastFragMsgId;
    struct UDPTransportFragInfo
    {
        uint64_t msgId;
        string data;
    };
    std::map<UDPTransportAddress, UDPTransportFragInfo> fragInfo;

    bool _SendMessageInternal(TransportReceiver *src,
                              const UDPTransportAddress &dst,
                              const Message &m,
                              size_t meta_len,
                              void *meta_data);
    bool SendMessageInternal(TransportReceiver *src,
                             const UDPTransportAddress &dst,
                             const Message &m) override;

    UDPTransportAddress
    LookupAddress(const specpaxos::ReplicaAddress &addr);
    UDPTransportAddress
    LookupAddress(const specpaxos::Configuration &cfg,
                  int groupIdx,
                  int replicaIdx) override;
    const UDPTransportAddress *
    LookupMulticastAddress(const specpaxos::Configuration *cfg) override;
    const UDPTransportAddress *
        LookupFCAddress(const specpaxos::Configuration *cfg) override;
    void ListenOnMulticastPort(const specpaxos::Configuration
                               *canonicalConfig);
    void OnReadable(int fd);
    void ProcessPacket(int fd, sockaddr_in sender, socklen_t senderSize,
                     char *buf, ssize_t sz);
    void OnTimer(UDPTransportTimerInfo *info);
    void OnManualEvent(UDPTransportTimerInfo *info);
    static void SocketCallback(evutil_socket_t fd,
                               short what, void *arg);
    static void TimerCallback(evutil_socket_t fd,
                              short what, void *arg);
    static void ManualCallback(evutil_socket_t fd,
                                  short what, void *arg);
    static void LogCallback(int severity, const char *msg);
    static void FatalCallback(int err);
    static void SignalCallback(evutil_socket_t fd,
                               short what, void *arg);
};

#endif  // _LIB_UDPTRANSPORT_H_
