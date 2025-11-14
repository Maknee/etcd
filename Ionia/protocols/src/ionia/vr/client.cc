// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * vr/client.cc:
 *   Skyros client
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

#include "common/client.h"
#include "common/request.pb.h"
#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "vr/client.h"
#include "vr/vr-proto.pb.h"

namespace specpaxos {
namespace vr {

VRClient::VRClient(const Configuration &config,
                   Transport *transport,
                   uint64_t clientid)
    : Client(config, transport, clientid)
{
    pendingRequest = NULL;
    pendingUnloggedRequest = NULL;
    lastReqId = 0;
    quorum = 0;

    /*
    requestTimeout = new Timeout(transport, 7000, [this]() {
            ResendRequest();
        });
    unloggedRequestTimeout = new Timeout(transport, 1000, [this]() {
            UnloggedRequestTimeoutCallback();
        });
    */
}

VRClient::~VRClient()
{
    if (pendingRequest) {
        delete pendingRequest;
    }
    if (pendingUnloggedRequest) {
        delete pendingUnloggedRequest;
    }
    delete requestTimeout;
    delete unloggedRequestTimeout;
}

void
VRClient::Invoke(const string &request,
                 continuation_t continuation)
{
    // XXX Can only handle one pending request for now
    if (pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    quorum = config.FastQuorumSize();
    // Notice("Sending request %s", request.c_str());
    if(request.c_str()[0] == 'r' || request.c_str()[0] == 'R'
        || request.c_str()[0] == 'e' || request.c_str()[0] == 'E') {
        // only one response expected for reads and non-nilext writes
        quorum = 1;
    }

    if (IMMEDIATE_WRITE_RESPOND)
    {
        // make writes and reads quorum 1
        quorum = 1;        
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;
    responses[lastReqId] = 0;
    leader_acked[lastReqId] = 0;

    auto& pending_request = pending_requests[reqId];
    pending_request.clientReqId = reqId;
    pending_request.continuation = std::move(continuation);

    proto::RequestMessage request_message;
    request_message.mutable_req()->set_op(std::move(request));
    request_message.mutable_req()->set_clientid(clientid);
    request_message.mutable_req()->set_clientreqid(reqId);
    if (request.c_str()[0] == 'r' || request.c_str()[0] == 'R')
    {
        if (!(transport->SendMessageToReplica(this,
                                              config.GetLeaderIndex(0),
                                              request_message)))
        {
            Warning("Failed to send read message to leader");
        }
    }
    else
    {
        if (IMMEDIATE_WRITE_RESPOND)
        {
            if (!(transport->SendMessageToReplica(this,
                                                config.GetLeaderIndex(0),
                                                request_message)))
            {
                Warning("Failed to send message to leader");
            }
        }
        else
        {
            transport->SendMessageToAll(this, request_message);
        }
    }
}

void
VRClient::InvokeUnlogged(int replicaIdx,
                         const string &request,
                         continuation_t continuation,
                         timeout_continuation_t timeoutContinuation,
                         uint32_t timeout)
{
    // XXX Can only handle one pending request for now
    if (pendingUnloggedRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;

    pendingUnloggedRequest = new PendingRequest(request, reqId, std::move(continuation));
    pendingUnloggedRequest->timeoutContinuation = std::move(timeoutContinuation);

    proto::UnloggedRequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(pendingUnloggedRequest->request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingUnloggedRequest->clientReqId);

    ASSERT(!unloggedRequestTimeout->Active());
    unloggedRequestTimeout->SetTimeout(timeout);
    unloggedRequestTimeout->Start();

    transport->SendMessageToReplica(this, replicaIdx, reqMsg);
}

void
VRClient::SendRequest()
{
    Panic("Deprecated function");
    thread_local proto::RequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(std::move(pendingRequest->request));
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingRequest->clientReqId);
    transport->SendMessageToAll(this, reqMsg);

    //requestTimeout->Reset();
}

void
VRClient::ResendRequest()
{
    Warning("Client timeout; resending request");
    SendRequest();
}


void
VRClient::ReceiveMessage(const TransportAddress &remote,
                         const std::string_view type,
                         const std::string_view data,
                         void *meta_data)
{
    const static auto replyName = proto::ReplyMessage::default_instance().GetDescriptor()->full_name();
    const static auto unloggedReplyName = proto::UnloggedReplyMessage::default_instance().GetDescriptor()->full_name();

    if (type == replyName) {
        ALLOCATE_MESSAGE(proto::ReplyMessage, reply);
        reply->ParseFromArray(data.data(), data.length());
        HandleReply(remote, *reply);
    } else if (type == unloggedReplyName) {
        ALLOCATE_MESSAGE(proto::UnloggedReplyMessage, unloggedReply);
        unloggedReply->ParseFromArray(data.data(), data.length());
        HandleUnloggedReply(remote, *unloggedReply);
    } else {
        Client::ReceiveMessage(remote, type, data, NULL);
    }
}

void
VRClient::HandleReply(const TransportAddress &remote,
                      const proto::ReplyMessage &msg)
{
    auto clientreqid = msg.clientreqid();
    responses[clientreqid]++;

    // proxy to check whether the responses are from the same view.
    assert(msg.view() == 0); 

    if((uint64_t) config.GetLeaderIndex(msg.view()) == msg.replicaidx()) {
        leader_acked[clientreqid] = 1;
    }

    auto& pending_request = pending_requests[clientreqid];

    if (clientreqid != pending_request.clientReqId) {
        Warning("Received reply for a different request %d != %d", clientreqid, pending_request.clientReqId);
        return;
    }

    Debug2("Client received reply");

    if(responses[clientreqid] >= quorum
        && leader_acked[clientreqid]) {
        //requestTimeout->Stop();
        //pending_request.continuation(pending_request.request, msg.reply());
        
        // return nothing since the client doesn't use the parameters anyways...
        pending_request.continuation("", "");
        responses.erase(clientreqid);
        leader_acked.erase(clientreqid);
        pending_requests.erase(clientreqid);
        // Notice("Got response: %s", msg.reply().c_str());
    }
}

void
VRClient::HandleUnloggedReply(const TransportAddress &remote,
                              const proto::UnloggedReplyMessage &msg)
{
    if (pendingUnloggedRequest == NULL) {
        Warning("Received unloggedReply when no request was pending");
        return;
    }

    Debug2("Client received unloggedReply");

    unloggedRequestTimeout->Stop();

    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    req->continuation(req->request, msg.reply());
    delete req;
}

void
VRClient::UnloggedRequestTimeoutCallback()
{
    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    Warning("Unlogged request timed out");

    unloggedRequestTimeout->Stop();

    req->timeoutContinuation(req->request);
}

} // namespace vr
} // namespace specpaxos
