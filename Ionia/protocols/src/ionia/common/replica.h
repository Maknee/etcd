// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replica.h:
 *   common interface to different replication protocols
 * 
 * Copyright 2021 Aishwarya Ganesan and Ramnatthan Alagappan
 *
 * Significant changes made to the code to implement Skyros
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

#ifndef _COMMON_REPLICA_H_
#define _COMMON_REPLICA_H_


#include <folly/concurrency/ConcurrentHashMap.h>
#include "lib/configuration.h"
#include "common/log.h"
#include "common/request.pb.h"
#include "lib/transport.h"
#include "lib/viewstamp.h"
#include "lib/workertasks.h"
#include "lib/naivequeue.h"
#include "vr/vr-proto.pb.h"
#include <assert.h>

#include "lib/db.h"
#include "lib/data_structure.h"

#include <map>
#include <mutex>
#include <filesystem>
#include <sys/types.h>
#include <pwd.h>

const int mutexBuckets = 10 * 1024 * 1024;

using folly::ConcurrentHashMap;

typedef std::pair<uint64_t, uint64_t> CXID;

namespace specpaxos {

class Replica;

enum ReplicaStatus {
    STATUS_NORMAL,
    STATUS_VIEW_CHANGE,
    STATUS_RECOVERING,
    STATUS_GAP_COMMIT
};

using DurIndex = uint64_t;

struct DurIndexAndMessage
{
	DurIndex index;
	DurIndexMessage message;
};

class AppReplica
{
private:
	static constexpr std::size_t opLength = 1;
    static constexpr std::size_t keyLength = 24;
	static constexpr std::size_t valueOffset = opLength + keyLength;

    // KV store.
	std::unique_ptr<DB> db = nullptr;

    std::atomic<uint64_t> durLogIndex = 0;

    // This is the durability log
	ThreadSafeMap<uint64_t, specpaxos::vr::proto::RequestMessage> durabilityLog;

    // Auxiliary to durability log
    ThreadSafeMap<CXID, uint64_t> cxidDurIndexMap;

    // Auxiliary to durability log
    ThreadSafeMap<std::string, uint64_t> keyInDurabilityLog;
    ConcurrentHashMap<std::string, uint64_t> keyToDurIndex;

    // The lastUpdateToKey is a supporting index structure
    // It facilitates a quick way of seeing what is the latest update to a key that is being read.
    ThreadSafeMap<string, CopyableAtomic<uint64_t>> lastUpdateToKey;

	// Reference to the execution log
	Log* log = nullptr;

	// Queue for pushing messages to background from durability threads
#ifdef USE_BACKGROUND_THREAD_SORT
	MultiProducerSingleConsumerQueue<DurIndexAndMessage> messageQueue{DURABILITY_MSG_QUEUE_PREALLOCATION};
#else
	RotatingVector2<DurIndexAndMessage, DURABILITY_MSG_QUEUE_PREALLOCATION_COLUMNS, DURABILITY_MSG_QUEUE_PREALLOCATION_ROWS> message_vector;
#endif
	// Current vector of durability indices and messages (that need to be sorted from the queue)
	std::vector<DurIndexAndMessage> messages;

	// To keep track of where end starts, so when popping from message queue adds to this end
	std::size_t messagesEnd = 0;

	// Keep track of what message index are we at (keeps track of what message index to expect next)
	std::size_t messageIndex = 0;

	bool is_leader = false;

    void apply(std::string_view key, std::string_view value) {
		std::string temp_value(100, 'x');
    	// Notice("Applying %s to store", key.c_str());
		db->Insert(DB_TABLE, key, std::string_view(temp_value));
    }

    std::optional<std::string> getFromStore(std::string_view key) {
		if (auto result = db->Read(DB_TABLE, key))
		{
			num_read_hits++;
			return result;
		}
		else
		{
			num_read_misses++;
			return std::nullopt;
		}
    }

	std::string getFromStoreOrDie(std::string_view key) {
		if (auto result = getFromStore(key))
		{
			return result.value();
		}
		else
		{
			// Panic("Key %s not found in store", key.data());
			return "NOTFOUND";
		}
	}

public:
    bool IsGet(std::string_view op) {
    	return !op.compare("r") || !op.compare("R");
    }

    bool IsSet(std::string_view op) {
    	return !op.compare("i") || !op.compare("I")
    			|| !op.compare("u") || !op.compare("U");
    }

    bool IsNonNilextWrite(std::string_view op) {
    	return !op.compare("e") || !op.compare("E");
    }

public:
    ConcurrentHashMap<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    
#ifdef REPLICA_USE_MESSAGE_ALLOCATOR
	RotatingMessageAllocatorThreadSafe<REPLICA_REQUEST_MESSAGE_ALLOCATOR_NUM_ROTATING_ALLOCATORS> request_message_allocator = RotatingMessageAllocatorThreadSafe<REPLICA_REQUEST_MESSAGE_ALLOCATOR_NUM_ROTATING_ALLOCATORS>(REPLICA_REQUEST_MESSAGE_ALLOCATOR_DEALLOCATION_THRESHOLD);
#endif
	// RotatingVector2<bool, 1000, 10 * 1000> keyInDurabilityLogVector;

	std::size_t num_read_hits = 0;
	std::size_t num_read_misses = 0;
	std::size_t num_sync_reads = 0;
	std::size_t num_immediate_reads = 0;

    AppReplica() {
		cxidDurIndexMap.reserve(DURABILITY_LOG_PREALLOCATION);
		durabilityLog.reserve(DURABILITY_LOG_PREALLOCATION);
		lastUpdateToKey.reserve(DURABILITY_LOG_PREALLOCATION);
		messages.resize(QUEUE_READ_ALLOCATION_LIMIT);

		Properties props;
		for (const auto& [k, v] : DB_PROPS)
		{
			props.SetProperty(k, v);
		}

		namespace fs = std::filesystem;
		auto splinterdb_exists = fs::exists(props.GetProperty("splinterdb.filename"));
		Warning("Splinterdb exists: %d", splinterdb_exists);

		for (const auto& [k, v] : DB_PROPS)
		{
			if (k == "db")
			{
				if (v == "memory")
				{
					db = std::make_unique<InMemoryDB>(props, splinterdb_exists);
				}
				else if (v == "splinter")
				{
					db = std::make_unique<SplinterDB>(props, splinterdb_exists);
					// if (splinterdb_exists)
					// {
					// 	Warning("SplinterDB preloaded size: %d", db->Size());
					// }
				}
			}
		}

		if (db == nullptr)
		{
			Panic("No db specified in DB_PROPS");
		}

		// Test load
		// const fs::path load_directory = homedir / "load";
		// Warning("Checking for load directory %s", fs::absolute(load_directory).string().c_str());
		// if (fs::exists(load_directory))
		// {
		// 	Warning("Found load... loading from %s", load_directory.string().c_str());
		// 	auto map_file = [](const char* fname, size_t& length, int& fd) -> const char*
		// 	{
		// 		fd = open(fname, O_RDONLY);
		// 		if (fd == -1)
		// 			Panic("open");

		// 		// obtain file size
		// 		struct stat sb;
		// 		if (fstat(fd, &sb) == -1)
		// 			Panic("fstat");

		// 		length = sb.st_size;

		// 		const char* addr = static_cast<const char*>(mmap(NULL, length, PROT_READ, MAP_PRIVATE, fd, 0u));
		// 		if (addr == MAP_FAILED)
		// 			Panic("mmap");

		// 		// TODO close fd at some point in time, call munmap(...)
		// 		return addr;
		// 	};

		// 	std::vector<std::thread> threads;
		// 	for (const auto& file : fs::recursive_directory_iterator(load_directory))
		// 	{
		// 		auto filename = file.path().string();
		// 		Warning("Loading %s", filename.c_str());
		// 		std::thread t([&, filename]{
		// 			db->Init();
		// 			size_t length;
		// 			int fd;
		// 			auto* f = map_file(filename.c_str(), length, fd);
		// 			auto ff = f;
		// 			auto l = f + length;

		// 			std::string temp_value(100, 'x');
		// 			while (f && f!=l)
		// 			{
		// 				const char* next_f = nullptr;
		// 				if ((next_f = static_cast<const char*>(memchr(f, '\n', l-f))))
		// 				{
		// 					std::string_view line(f, next_f - f);
		// 					auto key = line.substr(2);
		// 					db->Insert(DB_TABLE, key, temp_value);
		// 					f = next_f + 1;
		// 				}
		// 			}
		// 			munmap((void*)ff, length);
		// 			close(fd);
		// 			db->Close();
		// 		});
		// 		threads.push_back(std::move(t));
		// 	}
		// 	for (auto& t : threads)
		// 	{
		// 		t.join();
		// 	}
		// 	db->Shutdown();
		// 	Panic("Done loading");
		// }
    };

    void RegisterDurabilityThread() {
	db->Init();
    }

    void UnRegisterDurabilityThread() {
        db->Close();
    }


    void CloseDB(){
	db->Shutdown();
    }

    uint64_t GetDurabilityLogIndex() {
    	return durLogIndex.load();
    }

    auto GetDurabilityLogEntry(uint64_t index) {
		specpaxos::vr::proto::RequestMessage msg;
		if (!durabilityLog.if_contains(index, [&msg](const auto& v) { msg = v.second; }))
		{
			Panic("Could not find durability entry for index %d", index);
		}
		return msg;
    }

    void AddToQueue(DurIndex index, const specpaxos::vr::proto::RequestMessage& msg) {
		// Make a copy message managed by replica
#ifdef USE_BACKGROUND_THREAD_SORT

#ifdef REPLICA_USE_MESSAGE_ALLOCATOR
		auto copy = request_message_allocator.CreateViewed<specpaxos::vr::proto::RequestMessage>();
#else
		auto copy = new std::remove_pointer_t<DurIndexMessage>;
#endif
		*copy = msg;
		auto dur_index_and_message = DurIndexAndMessage{ index, copy };
		while (!messageQueue.try_enqueue(dur_index_and_message))
		{
			Warning("Message queue is full... waiting for background thread to consume messages");
			std::this_thread::yield();
		}
#endif
    }

	void DestroyRequestMessage(DurIndexMessage msg) {
#ifdef USE_BACKGROUND_THREAD_SORT
#ifdef REPLICA_USE_MESSAGE_ALLOCATOR
		request_message_allocator.DestroyMessageView(msg);
#else
		delete msg;
#endif
#endif
	}

#ifdef USE_BACKGROUND_THREAD_SORT
    std::tuple<std::vector<DurIndexAndMessage>&, std::size_t&, std::size_t&> GetAndDeleteFromQueue() {		
		// Check if past allocation limit
		if (messagesEnd + QUEUE_READ_LIMIT > QUEUE_READ_ALLOCATION_LIMIT)
		{
			Warning("Hit allocation limit %d + %d > %d", messagesEnd, QUEUE_READ_LIMIT, QUEUE_READ_ALLOCATION_LIMIT);
			return { messages, messagesEnd, messageIndex };
		}

		// Check if messagesEnd is able to get QUEUE_READ_LIMIT of elements
		// If not, allocate messagesEnd + QUEUE_READ_LIMIT to account for more elements
		if (messagesEnd + QUEUE_READ_LIMIT > messages.size())
		{
			// Try to make sure that this isn't called as this is a perf hit
			auto newMessageSize = messagesEnd + QUEUE_READ_LIMIT * 2;
			Warning("Trying to allocate more elements in message vector -- may incur a performance hit (trying to allocate %llu while messagesEnd is %llu and the messages vector is %llu)", QUEUE_READ_LIMIT, messagesEnd, messages.size());
			messages.resize(newMessageSize);
		}
		auto count = messageQueue.try_dequeue_bulk(&messages[messagesEnd], QUEUE_READ_LIMIT);
		messagesEnd += count;

		return { messages, messagesEnd, messageIndex };
    }
#else
    std::tuple<decltype(message_vector)&, std::size_t&, std::size_t&> GetAndDeleteFromQueue() {		
		return { message_vector, messagesEnd, messageIndex };
	}
#endif

	bool IsNilext(const specpaxos::vr::proto::RequestMessage& msg) {
		const auto& req = msg.req();
		const std::string& data = req.op();
		std::string_view data_view(data);

		std::string_view op = data_view.substr(0, opLength);
		return IsSet(op);
	}

	/*
	 * The AppUpcall encompasses both makedurable and read upcalls to the storage system
	 * If this is a nilext operation, then the operation is added to the durability log
	 * If this is a read operation, the readRes which is an out parameter contains the result of the read
	 * If the read requires a sync, syncOrder is set to true.
	 * Note that clients do not send the non-nilext operations to the durability server; they are
	 * immediately ordered by sending to consensus.
	*/

	DurIndex AppUpcall(const specpaxos::vr::proto::RequestMessage& msg, bool &syncOrder, string &readRes) {
		syncOrder = false;

		const auto& req = msg.req();
		const std::string& data = req.op();
		std::string_view data_view(data);

		auto op = data_view.substr(0, opLength);
		auto kvKey = data_view.substr(opLength, keyLength);

		std::pair<uint64_t, uint64_t> tableKey = std::make_pair(
				req.clientid(), req.clientreqid());

		if (IsSet(op)) {
			//Notice("Adding to durabiity set and lastUpdateToKey %lu,%lu: %s,%s", msg.req().clientid(),  msg.req().clientreqid(), op.c_str(), kvKey.c_str());

			if (IMMEDIATE_WRITE_RESPOND)
			{
				auto kvVal = data_view.substr(opLength + keyLength);
				apply(kvKey, kvVal);
				readRes = "durable-ack";
				return 0;
			}
			uint64_t myIndex = durLogIndex.fetch_add(1, std::memory_order::memory_order_relaxed);
#ifndef USE_BACKGROUND_THREAD_SORT
			if (is_leader)
			{
				auto dur_index_and_message = DurIndexAndMessage{ myIndex, msg };
				while (!message_vector.InsertUnsafe(myIndex, dur_index_and_message))
				{
					// Warning("Message vector is full... waiting for background thread to consume messages");
					std::this_thread::yield();
				}
			}
#endif
			durabilityLog.lazy_emplace_l(myIndex,
										[&](auto& v) { v.second = msg; },
										[&](const auto& ctor) { ctor(myIndex, msg); }
			);
			cxidDurIndexMap.lazy_emplace_l(tableKey,
										  [&](auto& v) { v.second = myIndex; },
										  [&](const auto& ctor) { ctor(tableKey, myIndex); }
			);
			lastUpdateToKey.lazy_emplace_l(kvKey,
										  [&](auto& v)
										  {
											// Do this but atomically
										  	// if (v.second < myIndex)
											// {
											// 	v.second = myIndex;
											// }
											auto& last_index = v.second;
											uint64_t target_value = myIndex;
											uint64_t last_value = last_index.load(std::memory_order::memory_order_relaxed);
											while (last_value < target_value && !last_index.compare_exchange_weak(last_value, target_value, std::memory_order_release, std::memory_order::memory_order_relaxed))
											{
											}
										  },
										  [&](const auto& ctor) { ctor(kvKey, myIndex); }
			);

			readRes = "durable-ack";
			return myIndex;
		} else if (IsGet(op)) {
			if (!durabilityLog.empty())
			{
				uint64_t index{};
				if (lastUpdateToKey.if_contains(kvKey,
					[&](const auto& v)
					{
						index = v.second.load(std::memory_order::memory_order_acquire);
					})
				)
				{
					if (durabilityLog.if_contains(index, [](const auto& v) {}))
					{
						// pending update unordered.
						syncOrder = true;
						readRes = "ordernowread!";
						num_sync_reads++;
					}
					else
					{
						// present in lastupdatetokey but not durability log
						// which means the update must have been applied to the store
						readRes = getFromStoreOrDie(kvKey);
						num_immediate_reads++;
					}
				}
				else
				{
					// no update to the key; so directly read.
					readRes = getFromStoreOrDie(kvKey);
					num_immediate_reads++;
				}
			}
			else
			{
				// No entries in durlog; so, no pending updates and thus can read directly.
				readRes = getFromStoreOrDie(kvKey);
				num_immediate_reads++;
			}

			// if (durabilityLog.size() > 0) {

			// 	// TODO: fix find -> contains_if for thread safety
			// 	if (lastUpdateToKey.find(kvKey) != lastUpdateToKey.end()) {
			// 		uint64_t index = lastUpdateToKey[kvKey];
			// 		//syncOrder = (durabilityLog.find(index) != durabilityLog.end());

			// 		if (!syncOrder) {
			// 			// present in lastupdatetokey but not durability log
			// 			// which means the update must have been applied to the store
			// 			// Notice("Retrieving from store %s", kvKey.c_str());
			// 			// assert(kvStore.find(kvKey) != kvStore.end());
			// 			// readRes = (kvStore.find(kvKey))->second;
			// 			assert(0);
			// 		} else {
			// 			// pending update unordered.
			// 			readRes = "ordernowread!";
			// 		}
			// 	} else {
			// 		// no update to the key; so directly read.
			// 		readRes = getFromStore(kvKey);
			// 	}
			// } else { // No entries in durlog; so, no pending updates and thus can read directly.
			// 	readRes = getFromStore(kvKey);
			// }
		} else if (IsNonNilextWrite(op)) {
			// Should not happen
			assert(0);
		} else {
			Panic("Unknown operation to KV store app %s", msg.req().op().c_str());
		}

		return 0;
	};

	std::atomic<std::size_t> durLogI;
	std::atomic<std::size_t> durLogIncrease;

	bool CheckIfKeyIsInDurLog(const std::string& key, auto& index)
	{
		// if (keyInDurabilityLog.erase_if(key,
		// 								[&](auto& v)
		// 								{
		// 									if (v.second <= index)
		// 									{
		// 										return true;
		// 									}
		// 									else
		// 									{
		// 										return false;
		// 									}
		// 								}
		// 								))
		// {
		// 	return false;
		// }
		// else
		// {
		// 	return true;
		// }

		// const auto& [entry, valid] = keyInDurabilityLogVector.GetEntry2(index);
		// if (valid)
		// {
		// 	keyInDurabilityLogVector.Delete(index);
		// 	//durLogI.fetch_sub(1, std::memory_order::memory_order_relaxed);
		// 	return false;
		// }
		// else
		// {
		// 	return true;
		// }
        // const auto& [entry, valid] = message_vector.GetEntry2(index);
		// return entry.has_value() && valid;
		if (durabilityLog.if_contains(index, [](const auto& v) {}))
		{
			// Notice("Checking for index %d", index);
			return true;
		}
		else
		{
			return false;
		}
		// if (auto found = keyToDurIndex.find(key); found != std::end(keyToDurIndex))
		// {
		// 	if (found->second <= index)
		// 	{
		// 		keyToDurIndex.erase(found);
		// 	}
		// 	return true;
		// }
		// else
		// {
		// 	return false;
		// }
		// if (cxidDurIndexMap.if_contains(index, [](const auto& v) {}))
		// {
		// 	// Notice("Checking for index %d", index);
		// 	std::this_thread::sleep_for(std::chrono::milliseconds(1));
		// 	return true;
		// }
		// else
		// {
		// 	return false;
		// }
	}

	std::optional<std::string> CheckIfKeyIsPendingUpdate(std::string_view kvKey, DurIndex index)
	{
		if (durabilityLog.if_contains(index, [](const auto& v) {}))
		{
			// pending update unordered.
			return std::nullopt;
		}
		else
		{
			// present in lastupdatetokey but not durability log
			// which means the update must have been applied to the store
			auto result = getFromStore(kvKey);
			if (result == std::nullopt)
			{
				return "NOTFOUND";
				// Panic("Pending key %s was not found in store, but deleted from durability log", kvKey.data());
			}
			return result.value();
		}
	}

	virtual void clearDurabilityLog() {
		durabilityLog.clear();
		durLogIndex = 0;
		//Notice("Clearing DurabilityLog");
	};


	// Used during recovery and viewchange
	virtual void addToDurabilityLogInOrder(std::vector<Request> requests) {
		assert(0); // for perf testing

		/*Notice("addToDurabilityLogInOrder");
		for (auto it : requests) {
			std::pair<uint64_t, uint64_t> tableKey = std::make_pair(it.clientid(),
					it.clientreqid());
			string kvKey = it.op().substr(opLength, keyLength);
			specpaxos::vr::proto::RequestMessage *requestMessage = new  specpaxos::vr::proto::RequestMessage();
			requestMessage->set_allocated_req(&it);
			//durabilityLog.insert_or_assign(tableKey, std::make_pair(durLogIndex++, *requestMessage));
			//lastUpdateToKey.insert_or_assign(kvKey, tableKey);
			//Notice("%lu,%lu:", requestMessage->req().clientid(), requestMessage->req().clientreqid());
		} */
	};

	// get the durability log in order.
	// Used during recovery and viewchange
	virtual std::vector<Request> GetDurabilityLogInOrder() {
		assert(0); // for perf testing

		/*std::vector<std::pair<uint64_t, specpaxos::vr::proto::RequestMessage>> toSort;
		for (auto &it : durabilityLog) {
			toSort.push_back(std::make_pair(it.second.first, it.second.second));
		}

		// we sort the durability log by position.
		sort(toSort.begin(), toSort.end(),
				[=](
						std::pair<uint64_t, specpaxos::vr::proto::RequestMessage> &a,
						std::pair<uint64_t, specpaxos::vr::proto::RequestMessage> &b) {
					return a.first < b.first;
				}
		);

		
		for (auto &it : toSort) {
			toReturn.push_back(it.second.req());
			Notice("DL: %lu, %lu, %lu", it.first, it.second.req().clientid(), it.second.req().clientreqid());
		}*/

		std::vector<Request> toReturn;
		return toReturn;
	};

	// Invoke callback on all replicas
	// This is called when a request is committed (either synchronously or in background)
    virtual void ReplicaUpcall(opnum_t opnum, const Request &req, string &str2,
                               void *arg = nullptr, void *ret = nullptr) {
		const std::string& data = req.op();
		std::string_view data_view(data);
    	std::string_view op = data_view.substr(0, opLength);
    	std::string_view kvKey = data_view.substr(opLength, keyLength);
    	if(!IsGet(op)) {
			std::size_t otherLength = opLength + keyLength;
			std::size_t valLength = data_view.size()  - otherLength;
			std::string_view kvVal = data_view.substr(otherLength, valLength);
			apply(kvKey, kvVal);
			str2 = "";
			CXID creqId = std::make_pair(req.clientid(), req.clientreqid());

			uint64_t indexToDelete;
			if (cxidDurIndexMap.erase_if(creqId,
									     [&](auto& v)
										 {
											indexToDelete = v.second;
											return true;
										 }
										 ))
			{
				if (durabilityLog.erase_if(indexToDelete,
									       [&](auto& v) { return true; }
										   ))
				{
					// Got deleted
				}
				else
				{
					Panic("durabilityLog does not have %d", indexToDelete);
				}

				// keyInDurabilityLog.lazy_emplace_l(kvKey,
				// 							[&](auto& v) { v.second = std::max(opnum, v.second); },
				// 							[&](const auto& ctor) { ctor(kvKey, opnum); }
				// );
				// auto& [entry, valid] = keyInDurabilityLogVector.GetEntry2(indexToDelete);
				// valid = true;
				// *entry = true;
				// durLogI.fetch_add(1, std::memory_order::memory_order_relaxed);
				// if (auto found = keyToDurIndex.find(std::string(kvKey)); found != std::end(keyToDurIndex))
				// {
				// 	found->second = std::max(found->second, indexToDelete);
				// }
				// else
				// {
				// 	keyToDurIndex.emplace(kvKey, indexToDelete);
				// }
			}
			else
			{
				// Cannot panic here because the durability replica may be working on the requests slower than pulling the client requests
				// Panic("cxidDurIndexMap does not have %d - %d", req.clientid(), req.clientreqid());
			}

			if (lastUpdateToKey.erase_if(kvKey,
									     [&](auto& v)
										 {
											return true;
										 }
										 ))
			{
			}
			else
			{
				// Could be already deleted
				// Panic("lastUpdateToKey does not have %s", kvKey.data());
			}

			// Trim log
			// log->SetExecuted(opnum, true);
			log->Trim(opnum);

			//uint64_t indexToDelete = cxidDurIndexMap[creqId];
			//cxidDurIndexMap.erase(creqId);
			//durabilityLog.erase(indexToDelete);
			
    	} else {
    		// populate read result
    		str2 = getFromStoreOrDie(kvKey);
    	}
    };

    // Rollback callback on failed speculative operations
    virtual void RollbackUpcall(opnum_t current, opnum_t to, const std::map<opnum_t, string> &opMap) { };

    virtual void CommitUpcall(opnum_t) { };

    // Invoke call back for unreplicated operations run on only one replica
    virtual void UnloggedUpcall(const string &str1, string &str2) { };

	struct OpKeyValueView
	{
		std::string_view op;
		std::string_view key;
		std::string_view value;
	};

	OpKeyValueView ExtractOpKeyValueFromRequest(const Request& request) const
	{
		OpKeyValueView view;
		std::string_view op = request.op();
		view.op = op.substr(0, opLength);
		view.key = op.substr(opLength, keyLength);
		view.value = op.substr(opLength + keyLength);
		return view;
	}

	// Execution queue related
	struct ExecutionData
	{
		opnum_t lastCommitted;
		const Request* request;
		uint64_t view;
		uint64_t opnum;
		uint64_t clientreqid;
		uint32_t replicaIdx;
		uint64_t clientid;
	};

    std::vector<SingleProducerSingleConsumerQueue<ExecutionData>> executionQueues;

	void SetNumDurabilityReplicas(std::size_t numDurabilityReplicas);
	std::size_t GetNumDurabilityReplicas() const;
	
	void SendDataToExecutionQueue(std::size_t index, ExecutionData data);
	std::optional<ExecutionData> PullDataFromExecutionQueue(std::size_t index);

	std::size_t GetKVSize() const { if (db) { return db->ApproxSize(); } else { return 0; } }
	auto GetDurabilityLogSize() const { return durabilityLog.size(); }

	void SetLog(Log* log) { this->log = log; }
	Log* GetLog() { return log; }
	void SetLeader(bool is_leader_) { is_leader = is_leader_; }
};

class Replica : public TransportReceiver
{
public:
    Replica(const Configuration &config, int groupIdx, int replicaIdx,
            bool initialize, Transport *transport, AppReplica *app);
    virtual ~Replica();

protected:
    void LeaderUpcall(specpaxos::vr::proto::RequestMessage msg, bool &syncOrder, string &readRes);
    void ReplicaUpcall(opnum_t opnum, const Request &req, string &res,
                       void *arg = nullptr, void *ret = nullptr);
    template<class MSG> void Execute(opnum_t opnum,
                                     const Request & msg,
                                     MSG &reply,
                                     void *arg = nullptr,
                                     void *ret = nullptr);
    void Rollback(opnum_t current, opnum_t to, Log &log);
    void Commit(opnum_t op);
    void UnloggedUpcall(const string &op, string &res);
    template<class MSG> void ExecuteUnlogged(const UnloggedRequest & msg,
                                               MSG &reply);

protected:
    Configuration configuration;
    int groupIdx;
    int replicaIdx;
    Transport *transport;
    AppReplica *app;
    ReplicaStatus status;
};

#include "replica-inl.h"

} // namespace specpaxos

#endif  /* _COMMON_REPLICA_H */
