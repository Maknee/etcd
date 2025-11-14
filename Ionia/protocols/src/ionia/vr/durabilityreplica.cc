// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 *
 * Copyright 2021 Aishwarya Ganesan and Ramnatthan Alagappan
 *
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

#include "common/replica.h"
#include "vr/durabilityreplica.h"
#include "vr/vr-proto.pb.h"

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"

#include <algorithm>
#include <random>

#define RDebug(fmt, ...) Debug2("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)

namespace specpaxos {
namespace vr {

using namespace proto;
VRDurabilityReplica:: VRDurabilityReplica(Configuration clientConfig, int myIdx, bool initialize,
              Transport *clientTransport, int batchSize,
              AppReplica *app_, Configuration internalConfig, Transport *internalTransport, std::size_t threadIndex_)
    : Replica(clientConfig, 0, myIdx, initialize, clientTransport, app_),
    internalConfig(internalConfig),
    internalTransport(internalTransport),
    threadIndex(threadIndex_)
{
    this->status = STATUS_NORMAL;
    this->view = 0;
    this->batchSize = batchSize;

    app->RegisterDurabilityThread();

    // we are a client to the consensus group
    // so register like a client would do
    internalTransport->Register(this, internalConfig, -2, -2);

    MakeDurabilityServerKnownMessage mdsm;
    mdsm.set_replicaidx(this->replicaIdx);
    internalTransport->SendMessageToReplica(this, this->replicaIdx, mdsm);

    auto ExecutionCallback = [this]()
    {
        const auto reply_amount = ADAPTIVE_REPLY_EXECUTION_AMOUNT_ENABLED ? ADAPTIVE_REPLY_EXECUTION_AMOUNT_FUNCTION(app->GetDurabilityLogSize()) : REPLY_EXECUTION_LIMIT;
        // RATE_LIMITER_DURABILITY_LOG(app->GetDurabilityLogSize());
        for (auto i = 0; i < reply_amount; i++)
        {
            if (auto data = app->PullDataFromExecutionQueue(threadIndex))
            {
                ALLOCATE_MESSAGE_F(ReplyMessage, reply);
                auto [op, key, value] = app->ExtractOpKeyValueFromRequest(*data->request);
                std::string key_(key);

                Execute(data->lastCommitted, *data->request, reply);

        //         if (auto found = local_durability_log.find(key_); found != std::end(local_durability_log))
        //         {
        //             auto& index = found->second;
        //             index = std::max(index, data->opnum);
        //         }
        //         else
        //         {
        //             local_durability_log.emplace(key, data->opnum);
        //         }
		// if (local_durability_log.size() > 1000)
		// {
		// 	//std::erase_if(std::begin(local_durability_log), std::end(local_durability_log), [&](const auto& kv) { return kv.second < data->opnum - 500; });
		// }
                // auto indexToDelete = data->opnum;
				// local_durability_log.lazy_emplace_l(key_,
				// 							[&](auto& v) { v.second = std::max(indexToDelete, v.second); },
				// 							[&](const auto& ctor) { ctor(key_, indexToDelete); }
				// );
                reply.set_view(data->view);
                reply.set_opnum(data->opnum);
                reply.set_clientreqid(data->clientreqid);
                reply.set_replicaidx(data->replicaIdx);
                reply.set_clientid(data->clientid);

                auto iter = clientAddresses.find(reply.clientid());
                if (iter != std::end(clientAddresses))
                {
                    transport->SendMessage(this, *iter->second, reply);
                }
            }
            else
            {
                break;
            }
        }
    };

#ifdef USE_EVENTS_FOR_LOOP
    clientTransport->OnEventLoop(ExecutionCallback);
#else
    executionTimeout = new Timeout(transport, 0, ExecutionCallback, 0);
    executionTimeout->Start();
#endif
}

VRDurabilityReplica::~VRDurabilityReplica()
{
    
}

bool
VRDurabilityReplica::AmLeader() const
{
    return (configuration.GetLeaderIndex(view) == this->replicaIdx);
}

void
VRDurabilityReplica::ReceiveMessage(const TransportAddress &remote,
                          const std::string_view type,
                          const std::string_view data,
                          void *meta_data)
{
    const static auto requestMessageName = RequestMessage::default_instance().GetDescriptor()->full_name();
    const static auto replyMessageName = ReplyMessage::default_instance().GetDescriptor()->full_name();
    const static auto statusUpdateMessageName = StatusUpdateMessage::default_instance().GetDescriptor()->full_name();

    if (type == requestMessageName) {
        ALLOCATE_MESSAGE(RequestMessage, request);
        request->ParseFromArray(data.data(), data.length());
        HandleRequest(remote, *request);
    } else if (type == replyMessageName) {
        ALLOCATE_MESSAGE(ReplyMessage, reply);
        reply->ParseFromArray(data.data(), data.length());
        HandleConsensusReply(remote, *reply);
    } else if (type == statusUpdateMessageName) {
        ALLOCATE_MESSAGE(StatusUpdateMessage, sum);
        sum->ParseFromArray(data.data(), data.length());
        HandleStatusUpdate(remote, *sum);
    } else {
        RPanic("Received unexpected message type in VR proto: %s",
              type.data());
    }
}

void
VRDurabilityReplica::HandleConsensusReply(const TransportAddress &remote,
                      const proto::ReplyMessage &msg)
{
    auto iter = clientAddresses.find(msg.clientid());
    if (iter != clientAddresses.end()) {
        ALLOCATE_MESSAGE_F(ReplyMessage, reply);
        reply.set_reply(msg.reply());
        reply.set_view(msg.view());
        reply.set_opnum(msg.opnum()); 
        reply.set_clientreqid(msg.clientreqid());
        reply.set_replicaidx(msg.replicaidx());
        transport->SendMessage(this, *iter->second, reply);
        return;
    }


	Notice("Unknown client %lu; current table is:", msg.clientid());    	
    for (auto const& pair: clientAddresses) {
        Notice("clientid: %lu", pair.first);
    }

    // for reply for which we don't know the client?
    // that is fishy...crash.
    assert(0);
}

void 
VRDurabilityReplica::HandleStatusUpdate(const TransportAddress &remote,
                      const proto::StatusUpdateMessage &msg) {
	Notice("Received a statusupdate message with view %lu, status %u", msg.view(), msg.status());
	this->view = msg.view();
	this->status = static_cast<ReplicaStatus>(msg.status());
}

void VRDurabilityReplica::HandleRequest(const TransportAddress &remote,
		const RequestMessage &msg) {

	if (status != STATUS_NORMAL) {
		RNotice("Ignoring request due to abnormal status");
		return;
	}

	bool isNilext = app->IsNilext(msg);
	if (!isNilext) {
		if(!AmLeader())
			return;
	}

	bool syncOrder = false;
	string readRes = "";

    auto [op, key, value] = app->ExtractOpKeyValueFromRequest(msg.req());
    std::string op_(op);
    std::string key_(key);
	DurIndex durIndex = app->AppUpcall(msg, syncOrder, readRes);

    if (app->IsSet(op_))
    {
	// static std::atomic<uint64_t> indexxx;
    // auto limit = 1;
	// while (indexxx > limit && indexxx < app->durLogI.load())
	// {
	// 	auto amount = indexxx.fetch_add(1);
    //     	while (app->CheckIfKeyIsInDurLog(key_, amount))
	// 	{

	// 	}

        indices_to_check.push_back(durIndex);

        if (indices_to_check.size() >= 64 * 8)
        {
            auto checks = 0;
            auto total = 70;
            while (indices_to_check.size() > total && checks < total)
            {
                auto index = indices_to_check.front();
                indices_to_check.pop_front();

                auto attempts = 0;

                while (app->CheckIfKeyIsInDurLog(key_, index))
                {
                    // transport->RunOnce();
                    if (attempts > 0)
                    {
                        indices_to_check.push_back(index);
                        break;
                    }
                    attempts += 1;
                }
                checks++;
            }
        }
	}
	/*
        if (!keys_check.empty())
        {
            while (keys_check.size() > 1000)
            {
                auto to_find = keys_check.front();
                keys_check.pop_front();

                while (true)
                {
                    if (auto found = local_durability_log.find(to_find); found != std::end(local_durability_log))
                    {
                        auto& index = found->second;
			if (index <= durIndex)
			{
				local_durability_log.erase(found);
			}
                        break;
                    }
                    else
                    {
                        if (auto data = app->PullDataFromExecutionQueue(threadIndex))
                        {
                            ALLOCATE_MESSAGE_F(ReplyMessage, reply);
                            auto [op, key, value] = app->ExtractOpKeyValueFromRequest(*data->request);
                            std::string key_(key);

                            Execute(data->lastCommitted, *data->request, reply);

                            if (auto found = local_durability_log.find(key_); found != std::end(local_durability_log))
                            {
                                auto& index = found->second;
                                index = std::max(index, data->lastCommitted);
                            }
                            else
                            {
                                local_durability_log.emplace(key, data->lastCommitted);
                            }
                            // auto indexToDelete = data->opnum;
                            // local_durability_log.lazy_emplace_l(key_,
                            // 							[&](auto& v) { v.second = std::max(indexToDelete, v.second); },
                            // 							[&](const auto& ctor) { ctor(key_, indexToDelete); }
                            // );
                            reply.set_view(data->view);
                            reply.set_opnum(data->opnum);
                            reply.set_clientreqid(data->clientreqid);
                            reply.set_replicaidx(data->replicaIdx);
                            reply.set_clientid(data->clientid);

                            auto iter = clientAddresses.find(reply.clientid());
                            if (iter != std::end(clientAddresses))
                            {
                                transport->SendMessage(this, *iter->second, reply);
                            }
                        }
			else
			{
				transport->RunOnce();
			}
                    }
                }
            }
        }
        keys_check.push_back(key_);
	*/
        // while (durIndex >= app->durLogIncrease.load())
        // while (durIndex > 1000 && durIndex >= app->durLogIncrease.load() + 1000)
        // {
        //     transport->RunOnce();
        // }
        // if (app->durLogI.load() > 1 * 1000 * 1)
        // {
    	// 	// auto aa = app->durLogIncrease.fetch_add(1);
        //     auto aa = indices_to_check.front();
        //     indices_to_check.pop_front();
        //     while (app->CheckIfKeyIsInDurLog(key_, aa))
        //     {
        //         if (auto data = app->PullDataFromExecutionQueue(threadIndex))
        //         {
        //             ALLOCATE_MESSAGE_F(ReplyMessage, reply);
        //             auto [op, key, value] = app->ExtractOpKeyValueFromRequest(*data->request);
        //             std::string key_(key);

        //             Execute(data->lastCommitted, *data->request, reply);

        //             // if (auto found = local_durability_log.find(key_); found != std::end(local_durability_log))
        //             // {
        //             //     auto& index = found->second;
        //             //     index = std::max(index, data->opnum);
        //             // }
        //             // else
        //             // {
        //             //     local_durability_log.emplace(key, data->opnum);
        //             // }
        //             reply.set_view(data->view);
        //             reply.set_opnum(data->opnum);
        //             reply.set_clientreqid(data->clientreqid);
        //             reply.set_replicaidx(data->replicaIdx);
        //             reply.set_clientid(data->clientid);

        //             auto iter = clientAddresses.find(reply.clientid());
        //             if (iter != std::end(clientAddresses))
        //             {
        //                 transport->SendMessage(this, *iter->second, reply);
        //             }
        //         }
        //     }
        // }
        // else
        // {
        //     indices_to_check.push_back(durIndex);
        // }

	if (syncOrder) {
	// Order the operation now; add to consensus log by sending an internal message to consensus.
		if (readRes.compare("ordernowread!") == 0) {
			// Save the client's address
			// app->clientAddresses.insert_or_assign(msg.req().clientid(),
			//      std::unique_ptr<TransportAddress>(remote.clone()));

            // RequestMessage *msg2 = new RequestMessage();
            // ALLOCATE_MESSAGE_F(Request, request);
            // request.set_op(msg.req().op());
            // request.set_clientid(msg.req().clientid());
            // request.set_clientreqid(msg.req().clientreqid());
            // request.set_syncread(1);
            // msg2->set_allocated_req(&request);

			// internalTransport->SendMessageToReplica(this, this->replicaIdx, *msg2);
            
            // Wait for key to be updated
            while (true)
            {
                auto possible_update = app->CheckIfKeyIsPendingUpdate(key, durIndex);
                if (possible_update)
                {
                    readRes = possible_update.value();
                    break;
                }
                else
                {
                    transport->RunOnce();
                }
            }
            ALLOCATE_MESSAGE_F(ReplyMessage, reply);
            reply.set_reply(readRes);
            reply.set_view(this->view);
            reply.set_opnum(0); //cannot order now!
            reply.set_clientreqid(msg.req().clientreqid());
            reply.set_replicaidx(this->replicaIdx);
            transport->SendMessage(this, remote, reply);
		}

		return;
	} else {
		// nilext write or fast read directly respond
		ALLOCATE_MESSAGE_F(ReplyMessage, reply);
		reply.set_reply(readRes);
		reply.set_view(this->view);
		reply.set_opnum(0); //cannot order now!
		reply.set_clientreqid(msg.req().clientreqid());
		reply.set_replicaidx(this->replicaIdx);
		transport->SendMessage(this, remote, reply);
	}
	
	if (AmLeader()) {
		if (isNilext) {
            if (!IMMEDIATE_WRITE_RESPOND) {
                app->AddToQueue(durIndex, msg);
            }
            // internalTransport->TriggerManualEvent(1);
            // First thread that gets access, make it the thread that triggers manual event 
            // static auto first_thread_id = std::this_thread::get_id();
            // if (first_thread_id == std::this_thread::get_id()) {
    	    // 	internalTransport->TriggerManualEvent(1);
            // }
		}
	}

    // PE -- wait till key is not in durability log
    // if (app->IsSet(op_))
    // {
    //     std::pair<uint64_t, uint64_t> tableKey = std::make_pair(msg.req().clientid(), msg.req().clientreqid());
    //     while (app->CheckIfKeyIsInDurLog(key_, durIndex))
    //     {
    //         transport->RunOnce();

    //         // const auto reply_amount = ADAPTIVE_REPLY_EXECUTION_AMOUNT_ENABLED ? ADAPTIVE_REPLY_EXECUTION_AMOUNT_FUNCTION(app->GetDurabilityLogSize()) : REPLY_EXECUTION_LIMIT;
    //         // // RATE_LIMITER_DURABILITY_LOG(app->GetDurabilityLogSize());
    //         // for (auto i = 0; i < reply_amount; i++)
    //         // {
    //         //     if (auto data = app->PullDataFromExecutionQueue(threadIndex))
    //         //     {
    //         //         ALLOCATE_MESSAGE_F(ReplyMessage, reply);
    //         //         auto [op, key, value] = app->ExtractOpKeyValueFromRequest(*data->request);
    //         //         std::string key_(key);

    //         //         Execute(data->lastCommitted, *data->request, reply);

    //         //         // if (auto found = local_durability_log.find(key_); found != std::end(local_durability_log))
    //         //         // {
    //         //         //     auto& index = found->second;
    //         //         //     index = std::max(index, data->opnum);
    //         //         // }
    //         //         // else
    //         //         // {
    //         //         //     local_durability_log.emplace(key, data->opnum);
    //         //         // }
    //         //         // auto indexToDelete = data->opnum;
    //         //         // local_durability_log.lazy_emplace_l(key_,
    //         //         // 							[&](auto& v) { v.second = std::max(indexToDelete, v.second); },
    //         //         // 							[&](const auto& ctor) { ctor(key_, indexToDelete); }
    //         //         // );
    //         //         reply.set_view(data->view);
    //         //         reply.set_opnum(data->opnum);
    //         //         reply.set_clientreqid(data->clientreqid);
    //         //         reply.set_replicaidx(data->replicaIdx);
    //         //         reply.set_clientid(data->clientid);

    //         //         auto iter = clientAddresses.find(reply.clientid());
    //         //         if (iter != std::end(clientAddresses))
    //         //         {
    //         //             transport->SendMessage(this, *iter->second, reply);
    //         //         }
    //         //     }
    //         //     else
    //         //     {
    //         //         break;
    //         //     }
    //         // }
    //     }
    // }

    // // PE -- wait till key is not in durability log
    // if (app->IsSet(op_))
    // {
    //     // std::pair<uint64_t, uint64_t> tableKey = std::make_pair(msg.req().clientid(), msg.req().clientreqid());
    //     // while (app->CheckIfKeyIsInDurLog(key_, tableKey))
    //     // {
    //     //     transport->RunOnce();
    //     // }
    //     while (true)
    //     {
    //         if (local_durability_log.erase_if(key_,
    //                                         [&](auto& v)
    //                                         {
    //             // Warning("FOUND FOR %s %d %d %d", key_.c_str(), key_.length(), durIndex, v.second);
    //                                             return true;
    //                                             if (durIndex <= v.second)
    //                                             {
    //                                             	return true;
    //                                             }
    //                                             else
    //                                             {
    //                                             	return false;
    //                                             }
    //                                         }
    //                                         ))
    //         {
    //             break;
    //         }
    //         else
    //         {
    //             // std::this_thread::sleep_for(250ms);
    //             // Warning("LOOKING FOR %s %d %d", key_.c_str(), key_.length(), durIndex);
    //             auto ExecutionCallback = [this](auto durIndex)
    //             {
    //                 const auto reply_amount = ADAPTIVE_REPLY_EXECUTION_AMOUNT_ENABLED ? ADAPTIVE_REPLY_EXECUTION_AMOUNT_FUNCTION(app->GetDurabilityLogSize()) : REPLY_EXECUTION_LIMIT;
    //                 // RATE_LIMITER_DURABILITY_LOG(app->GetDurabilityLogSize());
    //                 for (auto i = 0; i < reply_amount; i++)
    //                 {
    //                     if (auto data = app->PullDataFromExecutionQueue(threadIndex))
    //                     {
    //                         ALLOCATE_MESSAGE_F(ReplyMessage, reply);
    //                         auto [op, key, value] = app->ExtractOpKeyValueFromRequest(*data->request);
    //                         std::string key_(key);

    //                         Execute(data->lastCommitted, *data->request, reply);

    //                         // if (auto found = local_durability_log.find(key_); found != std::end(local_durability_log))
    //                         // {
    //                         //     auto& index = found->second;
    //                         //     index = std::max(index, data->opnum);
    //                         // }
    //                         // else
    //                         // {
    //                         //     local_durability_log.emplace(key, data->opnum);
    //                         // }
    //             // Warning("EXECUTE FOR %s %d", key_.c_str(), key_.length());
    //                         local_durability_log.lazy_emplace_l(key_,
    //                                                     [&](auto& v) { v.second = std::max(durIndex, v.second); },
    //                                                     [&](const auto& ctor) { ctor(key_, durIndex); }
    //                         );
    //                         reply.set_view(data->view);
    //                         reply.set_opnum(data->opnum);
    //                         reply.set_clientreqid(data->clientreqid);
    //                         reply.set_replicaidx(data->replicaIdx);
    //                         reply.set_clientid(data->clientid);

    //                         auto iter = clientAddresses.find(reply.clientid());
    //                         if (iter != std::end(clientAddresses))
    //                         {
    //                             transport->SendMessage(this, *iter->second, reply);
    //                         }
    //                     }
    //                     else
    //                     {
    //                         break;
    //                     }
    //                 }
    //             };
    //             ExecutionCallback(durIndex);
    //             // transport->RunOnce();
    //         }
    //     }
    // }

    // PE -- wait till key is not in durability log
    // if (app->IsSet(op_))
    // {
    //     wait_ack = true;
    //     while (wait_ack)
    //     {
    //         auto ExecutionCallback = [this](auto durIndex)
    //         {
    //             const auto reply_amount = ADAPTIVE_REPLY_EXECUTION_AMOUNT_ENABLED ? ADAPTIVE_REPLY_EXECUTION_AMOUNT_FUNCTION(app->GetDurabilityLogSize()) : REPLY_EXECUTION_LIMIT;
    //             // RATE_LIMITER_DURABILITY_LOG(app->GetDurabilityLogSize());
    //             for (auto i = 0; i < reply_amount; i++)
    //             {
    //                 if (auto data = app->PullDataFromExecutionQueue(threadIndex))
    //                 {
    //                     ALLOCATE_MESSAGE_F(ReplyMessage, reply);
    //                     auto [op, key, value] = app->ExtractOpKeyValueFromRequest(*data->request);
    //                     std::string key_(key);

    //                     Execute(data->lastCommitted, *data->request, reply);
    //                     wait_ack = false;

    //                     // if (auto found = local_durability_log.find(key_); found != std::end(local_durability_log))
    //                     // {
    //                     //     auto& index = found->second;
    //                     //     index = std::max(index, data->opnum);
    //                     // }
    //                     // else
    //                     // {
    //                     //     local_durability_log.emplace(key, data->opnum);
    //                     // }
    //                     reply.set_view(data->view);
    //                     reply.set_opnum(data->opnum);
    //                     reply.set_clientreqid(data->clientreqid);
    //                     reply.set_replicaidx(data->replicaIdx);
    //                     reply.set_clientid(data->clientid);

    //                     auto iter = clientAddresses.find(reply.clientid());
    //                     if (iter != std::end(clientAddresses))
    //                     {
    //                         transport->SendMessage(this, *iter->second, reply);
    //                     }
    //                     break;
    //                 }
    //                 else
    //                 {
    //                     break;
    //                 }
    //             }
    //         };
    //         ExecutionCallback(durIndex);
    //         // transport->RunOnce();
    //     }
    // }
}
}
}
