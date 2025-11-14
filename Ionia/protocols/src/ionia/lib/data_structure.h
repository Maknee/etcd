#pragma once

#include "parallel_hashmap/phmap.h"
#include "third_party/concurrentqueue/concurrentqueue.h"
#include "third_party/readerwriterqueue/readerwriterqueue.h"
#include "third_party/unordered_dense/include/ankerl/unordered_dense.h"
#include "third_party/function2/include/function2/function2.hpp"
#include "rpc.h"
#include "lib/message.h"

#include <google/protobuf/message.h>
#include "vr/vr-proto.pb.h"

#include <execution>
#include <algorithm>

using namespace std::chrono_literals;

/////////////////////////////////////////////////////////
/// Config
/////////////////////////////////////////////////////////

// common/log.h
constexpr std::size_t LOG_SIZE_COLUMNS = 35 * 1000;
constexpr std::size_t LOG_SIZE_ROWS = 1 * 1000;

// common/replica.h
constexpr std::size_t QUEUE_READ_LIMIT = 1 * 1000 * 1000;
//constexpr auto QUEUE_READ_ALLOCATION_LIMIT = QUEUE_READ_LIMIT * 10000;
//constexpr std::size_t QUEUE_READ_ALLOCATION_LIMIT = 10 * 1000 * 1000;
constexpr std::size_t QUEUE_READ_ALLOCATION_LIMIT = 10 * 1000 * 1000;

constexpr std::size_t DURABILITY_LOG_PREALLOCATION = 1 * 1000 * 1000;

constexpr std::size_t DURABILITY_MSG_QUEUE_PREALLOCATION = 25 * 1000 * 1000;
constexpr std::size_t SORTED_DURABILITY_MSG_QUEUE_PREALLOCATION = 25 * 1000 * 1000;

constexpr std::size_t REPLICA_REQUEST_MESSAGE_ALLOCATOR_NUM_ROTATING_ALLOCATORS = 1 * 1000 * 10;
constexpr std::size_t REPLICA_REQUEST_MESSAGE_ALLOCATOR_DEALLOCATION_THRESHOLD = 1 * 1 * 1000;

constexpr std::size_t DURABILITY_MSG_QUEUE_PREALLOCATION_COLUMNS = 50;
constexpr std::size_t DURABILITY_MSG_QUEUE_PREALLOCATION_ROWS = 1000;

// vr/replica.cc
constexpr std::size_t BG_REPL_TIMEOUT_MICROSECONDS = 0;
constexpr bool BG_REPL_USE_BATCH_SIZE_AS_MESSAGE_LIMIT = true;
constexpr std::size_t BG_REPL_HANDLE_MESSAGE_LIMIT = 128;

// common/log.cc
constexpr bool LOG_TRIM_OVERWRITE = false;

// vr/durabilityreplica.cc
// constexpr std::size_t REPLY_EXECUTION_LIMIT = 16;
constexpr std::size_t REPLY_EXECUTION_LIMIT = 128;
constexpr bool ADAPTIVE_REPLY_EXECUTION_AMOUNT_ENABLED = false;
auto ADAPTIVE_REPLY_EXECUTION_AMOUNT_FUNCTION(auto durability_log_size)
{
    thread_local auto last_durability_log_size = 0;
    auto diff_durability_log_size = durability_log_size - last_durability_log_size;
    return diff_durability_log_size;
    // constexpr auto INDEX_THRESHOLD = 64;
    // return (durability_log_size + INDEX_THRESHOLD) / INDEX_THRESHOLD;
}

constexpr bool RATE_LIMITER_DURABILITY_LOG_ENABLED = false;
constexpr auto RATE_LIMITER_DURABILITY_LOG(auto durability_log_size)
{
    if constexpr (RATE_LIMITER_DURABILITY_LOG_ENABLED)
    {
        // make sure we don't blast way past what we should expect...
        constexpr auto DURABILITY_LOG_THRESHOLD = (((LOG_SIZE_COLUMNS * LOG_SIZE_ROWS) * 2) / 3);
        if (durability_log_size > DURABILITY_LOG_THRESHOLD)
        {
            // just sleep for a bit...
            std::this_thread::sleep_for(10ms);
        }
    }
}

constexpr bool IMMEDIATE_WRITE_RESPOND = false;

template<std::size_t N, typename INDEXER_TYPE>
class RotatingMessageAllocator;

template<std::size_t N>
using RotatingMessageAllocatorThreadSafe = RotatingMessageAllocator<N, std::atomic<std::size_t>>;

template<std::size_t N>
using RotatingMessageAllocatorThreadUnsafe = RotatingMessageAllocator<N, std::size_t>;

template<typename T>
class ManagedMessageView;

// #define ALLOCATE_MESSAGE_WITH_ALLOCATOR

#ifdef ALLOCATE_MESSAGE_WITH_ALLOCATOR

    constexpr std::size_t MESSAGE_ALLOCATOR_NUM_ROTATING_ALLOCATORS = 1000 * 10;
    constexpr std::size_t MESSAGE_ALLOCATOR_DEALLOCATION_THRESHOLD = 1 * 1 * 1000;

    using RotatingMessageAllocatorN = RotatingMessageAllocatorThreadUnsafe<MESSAGE_ALLOCATOR_NUM_ROTATING_ALLOCATORS>;

    template<typename T>
    struct ManagedMessageViewAllocation
    {
        explicit ManagedMessageViewAllocation() = default;
        explicit ManagedMessageViewAllocation(RotatingMessageAllocatorN* message_allocator_, ManagedMessageView<T> view_) : 
            message_allocator(std::move(message_allocator_)), view(std::move(view_))
        {

        }

        ~ManagedMessageViewAllocation()
        {
            if (view.GetUnderlying() != nullptr)
            {
                message_allocator->DestroyMessageView(view);
            }
        }
        
        ManagedMessageViewAllocation(const ManagedMessageViewAllocation& o) :
            message_allocator(o.message_allocator), view(o.view)
        {
            o.view = ManagedMessageView<T>();
        }

        ManagedMessageViewAllocation& operator=(const ManagedMessageViewAllocation& o)
        {
            message_allocator = o.message_allocator;
            view = o.view;
            o.view = ManagedMessageView<T>();
            return *this;
        }

        ManagedMessageViewAllocation(ManagedMessageViewAllocation&& o) :
            message_allocator(std::move(o.message_allocator)), view(std::move(o.view))
        {
            o.view = ManagedMessageView<T>();
        }

        ManagedMessageViewAllocation& operator=(ManagedMessageViewAllocation&& o)
        {
            message_allocator = o.message_allocator;
            view = o.view;
            o.view = ManagedMessageView<T>();
            return *this;
        }

        T& operator*() { return *view.GetUnderlying(); }
        const T& operator*() const { return *view.GetUnderlying(); }
        
        T* operator->() { return view.GetUnderlying(); }
        const T* operator->() const { return view.GetUnderlying(); }

        operator bool() const { return view.GetUnderlying() != nullptr; }

        RotatingMessageAllocatorN* message_allocator;
        ManagedMessageView<T> view;
    };

#define ALLOCATE_MESSAGE(T, v) \
    thread_local std::unique_ptr<RotatingMessageAllocatorN> v_ALLOCATOR = std::make_unique<RotatingMessageAllocatorN>(MESSAGE_ALLOCATOR_DEALLOCATION_THRESHOLD); \
    auto v_view = ManagedMessageViewAllocation<T>(&*v_ALLOCATOR, v_ALLOCATOR->CreateViewed<T>()); \
    auto* v = v_view.view.GetUnderlying(); \

#define ALLOCATE_MESSAGE_F(T, v) \
    thread_local std::unique_ptr<RotatingMessageAllocatorN> v_ALLOCATOR = std::make_unique<RotatingMessageAllocatorN>(MESSAGE_ALLOCATOR_DEALLOCATION_THRESHOLD); \
    auto v_view = ManagedMessageViewAllocation<T>(&*v_ALLOCATOR, v_ALLOCATOR->CreateViewed<T>()); \
    auto& v = *v_view.view; \

#define ALLOCATE_MESSAGE_UNDERLYING(T, v) \
    thread_local std::unique_ptr<RotatingMessageAllocatorN> v_ALLOCATOR = std::make_unique<RotatingMessageAllocatorN>(MESSAGE_ALLOCATOR_DEALLOCATION_THRESHOLD); \
    auto v = ManagedMessageViewAllocation<T>(&*v_ALLOCATOR, v_ALLOCATOR->CreateViewed<T>()); \

template<typename T>
using UnderlyingMessage = ManagedMessageViewAllocation<T>;

#else

#define ALLOCATE_MESSAGE(T, v) T v_; auto* v = &v_;
#define ALLOCATE_MESSAGE_F(T, v) T v_; auto& v = v_;
#define ALLOCATE_MESSAGE_UNDERLYING(T, v) auto v = std::make_unique<T>();

template<typename T>
using UnderlyingMessage = std::unique_ptr<T>;

#endif

// Taken from ankerl
inline uint64_t hash_combine(uint64_t seed, uint64_t val) {
    return seed ^ (val + uint64_t(0x9e3779b9) + (seed << 6U) + (seed >> 2U));
}

// #define USE_BACKGROUND_THREAD_SORT
#define LOG_DISABLE_CHECKS
#define USE_EVENTS_FOR_LOOP
#define REPLICA_USE_MESSAGE_ALLOCATOR

#ifdef USE_BACKGROUND_THREAD_SORT
    #ifdef REPLICA_USE_MESSAGE_ALLOCATOR
        using DurIndexMessage = ManagedMessageView<specpaxos::vr::proto::RequestMessage>;
    #else
        using DurIndexMessage = specpaxos::vr::proto::RequestMessage *;
    #endif
#else
    using DurIndexMessage = specpaxos::vr::proto::RequestMessage;
#endif
// lib/db.h
const inline std::map<std::string, std::string> DB_PROPS = {
  {"threadcount", "1"},
  {"dbname", "basic"},
  {"progress", "none"},

  //
  // Basicdb config defaults
  //
  {"basicdb.verbose", "0"},

  //
  // splinterdb config defaults
  //
  {"splinterdb.filename", "splinterdb.db"},
  {"splinterdb.cache_size_mb", "4096"},
  {"splinterdb.disk_size_gb", "256"},

  {"splinterdb.max_key_size", "24"},
  {"splinterdb.use_log", "0"},

  // All these options use splinterdb's internal defaults
  {"splinterdb.page_size", "0"},
  {"splinterdb.extent_size", "0"},
  {"splinterdb.io_flags", "0"},
  {"splinterdb.io_perms", "0"},
  {"splinterdb.io_async_queue_depth", "0"},
  {"splinterdb.cache_use_stats", "0"},
  {"splinterdb.cache_logfile", "0"},
  {"splinterdb.btree_rough_count_height", "0"},
  {"splinterdb.filter_remainder_size", "0"},
  {"splinterdb.filter_index_size", "0"},
  {"splinterdb.memtable_capacity", "0"},
  {"splinterdb.fanout", "0"},
  {"splinterdb.max_branches_per_node", "0"},
  {"splinterdb.use_stats", "0"},
  {"splinterdb.reclaim_threshold", "0"},

  // {"db", "memory"},
  {"db", "splinter"},
};

const inline std::string DB_TABLE = "db";

/////////////////////////////////////////////////////////
/// Data structures
/////////////////////////////////////////////////////////

constexpr size_t ParallelFlatHashMapSubmapSize = 8; //2**N submaps

template<typename T, typename T2>
using ThreadSafeMap = phmap::parallel_flat_hash_map<T, T2,
                                phmap::priv::hash_default_hash<T>,
                                phmap::priv::hash_default_eq<T>,
                                phmap::priv::Allocator<phmap::priv::Pair<const T, T2>>,
                                ParallelFlatHashMapSubmapSize,
                                std::mutex>;

//template<typename T, typename T2>
//using HashMap = phmap::flat_hash_map<T, T2>;

template<typename T, typename T2>
using HashMap = ankerl::unordered_dense::map<T, T2>;

template<typename T>
using SingleProducerSingleConsumerQueue = moodycamel::ReaderWriterQueue<T>;

template<typename T>
using MultiProducerSingleConsumerQueue = moodycamel::ConcurrentQueue<T>;

template<class T>
class CopyableAtomic : public std::atomic<T>
{
public:
    //defaultinitializes value
    CopyableAtomic() = default;

    constexpr CopyableAtomic(T desired) : 
        std::atomic<T>(desired) 
    {}

    constexpr CopyableAtomic(const CopyableAtomic<T>& other) :
        CopyableAtomic(other.load(std::memory_order_relaxed))
    {}

    CopyableAtomic& operator=(const CopyableAtomic<T>& other) {
        this->store(other.load(std::memory_order_acquire), std::memory_order_release);
        return *this;
    }
};

/////////////////////////////////////////////////////////
/// Protobuf
/////////////////////////////////////////////////////////

using MessageIndex = uint8_t;
constexpr size_t MAX_MESSAGE_INDEX = std::numeric_limits<MessageIndex>::max();

class MessageAllocator;

template<typename T>
class ManagedMessageView
{
public:
    explicit ManagedMessageView() = default;
    explicit ManagedMessageView(MessageAllocator* allocator_, T* t, std::size_t index_ = 0) :
        allocator(std::move(allocator_)),
        message(std::move(t)),
        index(std::move(index_)) 
    {
        static_assert(std::is_base_of_v<google::protobuf::Message, T>, "T must be a subclass of google::protobuf::Message");
    }

    ~ManagedMessageView() = default;
    ManagedMessageView(const ManagedMessageView& other)
        : ManagedMessageView(other.allocator, other.message, other.index)
    {

    }

    ManagedMessageView& operator=(const ManagedMessageView& other)
    {
        allocator = other.allocator;
        message = other.message;
        index = other.index;
        return *this;
    }

    ManagedMessageView(ManagedMessageView&& o) noexcept
        : ManagedMessageView(std::move(o.allocator), std::move(o.message), std::move(o.index))
    {
        
    }
 
    ManagedMessageView& operator=(ManagedMessageView&& o) noexcept
    {
        std::swap(allocator, o.allocator);
        std::swap(message, o.message);
        std::swap(index, o.index);
        return *this;
    }

    MessageAllocator* GetAllocator() { return allocator; }
    const MessageAllocator* GetAllocator() const { return allocator; }

    T* GetUnderlying() { return message; }
    const T* GetUnderlying() const { return message; }

    std::size_t GetIndex() const { return index; }

    T& operator*() { return *GetUnderlying(); }
    const T& operator*() const { return *GetUnderlying(); }
    
    T* operator->() { return GetUnderlying(); }
    const T* operator->() const { return GetUnderlying(); }

private:
    MessageAllocator* allocator = nullptr;
    T* message = nullptr;
    std::size_t index = 0;
};

template<typename T>
class ManagedMessage
{
public:
    explicit ManagedMessage() = default;
    explicit ManagedMessage(MessageAllocator* allocator_, T* t) :
        allocator(std::move(allocator_)),
        message(std::move(t))
    {
        static_assert(std::is_base_of_v<google::protobuf::Message, T>, "T must be a subclass of google::protobuf::Message");
    }

    explicit ManagedMessage(ManagedMessageView<T> view)
        : ManagedMessage(view.GetAllocator(), view.GetUnderlying())
    {
    }

    ~ManagedMessage()
    {
        if (allocator && message)
        {
            allocator->DestroyMessage(message);
        }
    }

    ManagedMessage(const ManagedMessage& other) = delete;
    ManagedMessage& operator=(const ManagedMessage& other) = delete;
    ManagedMessage(ManagedMessage&& o) noexcept
        : ManagedMessage(std::move(o.allocator), std::move(o.message))
    {
        
    }
 
    ManagedMessage& operator=(ManagedMessage&& o) noexcept
    {
        std::swap(allocator, o.allocator);
        std::swap(message, o.message);
        return *this;
    }

    MessageAllocator* GetAllocator() { return allocator; }
    const MessageAllocator* GetAllocator() const { return allocator; }

    T* GetUnderlying() { return message; }
    const T* GetUnderlying() const { return message; }

    T& operator*() { return *GetUnderlying(); }
    const T& operator*() const { return *GetUnderlying(); }
    
    T* operator->() { return GetUnderlying(); }
    const T* operator->() const { return GetUnderlying(); }

    ManagedMessageView<T> GetView() const
    {
        return ManagedMessageView<T>(allocator, message);
    }

private:
    MessageAllocator* allocator = nullptr;
    T* message = nullptr;
};

class MessageAllocator
{
public:
    explicit MessageAllocator() = default;
    explicit MessageAllocator(std::size_t deallocate_threshold_) :
        deallocate_threshold(deallocate_threshold_)
    {
    }

    template<typename T, typename... Args>
    inline ManagedMessage<T> Create(Args... args)
    {
        T* message = CreateMessage<T>(std::forward<Args>(args)...);
        return ManagedMessage(this, message);
    }

    template<typename T, typename... Args>
    inline ManagedMessageView<T> CreateViewed(Args... args)
    {
        T* message = CreateMessage<T>(std::forward<Args>(args)...);
        return ManagedMessageView(this, message);
    }

    template<typename T>
    inline ManagedMessage<T> Manage(T* t)
    {
        return ManagedMessage(this, t);
    }

    template<typename T>
    inline ManagedMessage<T> Manage(ManagedMessageView<T>* t)
    {
        return ManagedMessage(t);
    }

    template<typename T, typename... Args>
    inline T* CreateMessage(Args... args)
    {
        static_assert(std::is_base_of_v<google::protobuf::Message, T>, "T must be a subclass of google::protobuf::Message");

        // if (deallocate_threshold > 0)
        // {
        //     while (deallocating) { std::this_thread::yield(); }
        // }

        // Check if there is a free message
        // auto index = T::default_instance().GetDescriptor()->index();
        // auto& free_messages = index_to_free_messages[index];
        // if (!free_messages.empty())
        // {
        //     google::protobuf::Message* message = free_messages.back();
        //     free_messages.pop_back();
        //     return reinterpret_cast<T*>(message);
        // }

        // Perform allocation
        //T* message = google::protobuf::Arena::Create<T>(&arena, std::forward<Args>(args)...);
        T* message = google::protobuf::Arena::CreateMessage<T>(&arena);
        return message;
    }

    template<typename T>
    void DestroyMessage(T* message)
    {
        if (!message)
        {
            return;
        }
        // (*message).~T();

        // auto index = T::default_instance().GetDescriptor()->index();
        // auto& free_messages = index_to_free_messages[index];
        // free_messages.emplace_back(std::move(message));

        // if (deallocate_threshold > 0)
        // {
        //     while (deallocating) { std::this_thread::yield(); }
    
        //     if (num_deallocations > deallocate_threshold)
        //     {
        //         deallocating = true;
        //         arena.Reset();
        //         num_deallocations = 0;
        //         deallocating = false;
        //     }
        //     else
        //     {
        //         num_deallocations++;
        //     }
        // }
    }

    void Reset()
    {
        // Not thread safe
        arena.Reset();
    }

    auto& Arena()
    {
        return arena;
    }

private:
    // std::atomic<bool> deallocating = false;
    // std::atomic<std::size_t> num_deallocations = 0;
    std::size_t deallocate_threshold = 0;
    google::protobuf::Arena arena;
    std::array<std::vector<google::protobuf::Message*>, MAX_MESSAGE_INDEX> index_to_free_messages;
};

template <typename>
constexpr bool is_atomic_v = false;

template <typename T>
constexpr bool is_atomic_v<std::atomic<T>> = true;

template<std::size_t N = 1, typename INDEXER_TYPE = std::atomic<std::size_t>>
class RotatingMessageAllocator
{
public:
    explicit RotatingMessageAllocator() = default;
    explicit RotatingMessageAllocator(std::size_t deallocate_threshold_) :
        deallocate_threshold(deallocate_threshold_)
    {
    }

    inline auto GrabIndex(INDEXER_TYPE& index)
    {
        if constexpr(is_atomic_v<INDEXER_TYPE>)
        {
            return index.load();
        }
        else
        {
            return index;
        }
    }

    inline auto IncIndex(INDEXER_TYPE& index)
    {
        if constexpr(is_atomic_v<INDEXER_TYPE>)
        {
            return index.fetch_add(1, std::memory_order_relaxed) + 1;
        }
        else
        {
            return ++index;
        }
    }


    template<typename T, typename... Args>
    inline ManagedMessageView<T> CreateViewed(Args... args)
    {
        auto original_index = current_index;
        auto current_allocations = IncIndex(allocations[original_index]);
        if (current_allocations >= deallocate_threshold)
        {
            auto new_index = original_index;
            new_index++;
            if (new_index == N)
            {
                new_index = 0;
            }
            current_index = new_index;
            // double check
            if (allocations[current_index] > deallocate_threshold)
            {
                Panic("RotatingMessageAllocator: Reached deallocation threshold for [%d] %d > %d", current_index, GrabIndex(allocations[current_index]), current_allocations);
            }
        }

        auto& message_allocator = message_allocators[original_index];
        auto& arena = message_allocator.Arena();
        T* message = google::protobuf::Arena::CreateMessage<T>(&arena);
        return ManagedMessageView<T>(&message_allocator, message, original_index);
    }

    template<typename T>
    void DestroyMessageView(ManagedMessageView<T> message_view)
    {
        auto* allocator = message_view.GetAllocator();
        auto index = message_view.GetIndex();
        auto current_allocations = GrabIndex(allocations[index]);
        auto current_deallocations = IncIndex(deallocations[index]);
        if (current_deallocations >= deallocate_threshold)
        {
            if (current_deallocations == current_allocations)
            {
                allocations[index] = 0;
                deallocations[index] = 0;
                allocator->Reset();
                // Warning("Resetting allocator [%d] %d == %d", index, GrabIndex(allocations[index]), GrabIndex(deallocations[index]));
            }
            if (current_deallocations > current_allocations)
            {
                Panic("RotatingMessageAllocator: Deallocations exceeded allocations for [%d] %d > %d", index, GrabIndex(deallocations[index]), GrabIndex(allocations[index]));
            }
        }
    }

    auto& Arena()
    {
        return message_allocators[current_index].Arena();
    }

    void Reset(std::size_t index)
    {
        // Not thread safe
        message_allocators[index].Arena().Reset();
    }

private:
    std::size_t deallocate_threshold = 0;

    std::size_t current_index = 0;
    
    std::mutex set_next_index_mutex;
    std::array<INDEXER_TYPE, N> allocations;
    std::array<INDEXER_TYPE, N> deallocations;
    std::array<MessageAllocator, N> message_allocators;
};

/////////////////////////////////////////////////////////
/// Custom data structures
/////////////////////////////////////////////////////////

template<typename K, typename V>
class VectorMap
{
public:
    explicit VectorMap()
    {
        static_assert(std::is_same_v<K, uint64_t>, "K must be uint64_t");
    }

    auto find(K k)
    {
        if (k >= data.size())
        {
            return end();
        }
        return Iterator(data, k);
    }

    auto begin()
    {
        return Iterator(data, 0);
    }

    const auto begin() const
    {
        return Iterator(data, 0);
    }

    auto end()
    {
        return Iterator(data, data.size());
    }

    const auto end() const
    {
        return Iterator(data, data.size());
    }

    V& operator[](K k)
    {
        if (k >= data.size())
        {
            data.resize(k + 1);
        }
        return data[k];
    }

    const auto size() const
    {
        return data.size();
    }

    bool empty() const
    {
        return size() == 0;
    }

private:
    struct Iterator
    {
        Iterator(std::vector<V>& data, K k) : data(data), kv(k, data[k]) {}

        auto& operator*() const { return kv; }
        auto* operator->() { return &kv; }
        Iterator& operator++() { kv.first++; kv.second = data[kv.first]; return *this; }
        Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }
        friend bool operator== (const Iterator& a, const Iterator& b) { return a.kv.first == b.kv.first; };
        friend bool operator!= (const Iterator& a, const Iterator& b) { return a.kv.first != b.kv.first; };

        std::pair<K, V&> kv;
        std::vector<V>& data;
    };

    std::vector<V> data;
};

struct RotatingVectorHandle
{
    std::size_t index;
};

struct RowColumnIndex
{
    std::size_t row;
    std::size_t column;
};

template<typename T>
struct RotatingVectorEntry
{
    T t;
    bool valid = false;
};

template<typename T, std::size_t N = 1, std::size_t ROWS = 4, bool INITIALIZE = false>
class RotatingVector2
{
public:
    explicit RotatingVector2()
    {
        static_assert(N > 0, "N must be greater than 0");
        static_assert(ROWS > 0, "ROWS must be greater than 0");

        if constexpr(INITIALIZE)
        {
            std::for_each(std::execution::par_unseq, std::begin(data), std::end(data), [](auto& row)
            {
                std::for_each(std::execution::par_unseq, std::begin(row), std::end(row), [](auto& entry)
                {
                    entry.t = T{};
                    entry.valid = false;
                });
            });
        }
        else
        {
            std::for_each(std::execution::par_unseq, std::begin(data), std::end(data), [](auto& row)
            {
                std::for_each(std::execution::par_unseq, std::begin(row), std::end(row), [](auto& entry)
                {
                    entry.t = std::nullopt;
                    entry.valid = false;
                });
            });
        }
    }

    inline RowColumnIndex GetRowColumnIndex(std::size_t index) const
    {
        return RowColumnIndex{(index / N) % ROWS, index % N};
    }

    bool Insert(std::size_t index, T&& value)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        if (entry || valid)
        {
            return false;
        }
        entry = std::forward<T>(value);
        valid = true;
        return true;
    }

    void InsertNoCheck(std::size_t index, T&& value)
    {
        if (!Insert(index, std::forward<T>(value)))
        {
            Panic("RotatingVector: InsertPanic failed");
        }
    }

    T& InsertNoAllocation(std::size_t index)
    {
        // Retry...
        while (true)
        {
            auto [row, column] = GetRowColumnIndex(index);
            auto& [entry, valid] = data[row][column];
            if (valid)
            {
                Warning("RotatingVector: InsertNoAllocation failed");
                std::this_thread::sleep_for(10ms);
                continue;
            }
            if (!entry)
            {
                Warning("RotatingVector: InsertNoAllocation failed -- entry invalid");
                std::this_thread::sleep_for(10ms);
                continue;
            }
            valid = true;
            return *entry;
        }

        // auto [row, column] = GetRowColumnIndex(index);
        // auto& [entry, valid] = data[row][column];
        // if (valid)
        // {
        //     Panic("RotatingVector: InsertNoAllocation failed");
        // }
        // if (!entry)
        // {
        //     Panic("RotatingVector: InsertNoAllocation failed -- entry invalid");
        // }
        // valid = true;
        // return *entry;
    }

    bool InsertUnsafe(std::size_t index, const T& value)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        if (entry || valid)
        {
            return false;
        }
        entry = std::move(value);
        valid = true;
        return true;
    }

    RotatingVectorEntry<std::optional<T>>& GetEntry2(std::size_t index)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& entry = data[row][column];
        return entry;
    }

    const RotatingVectorEntry<std::optional<T>>& GetEntry2(std::size_t index) const
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& entry = data[row][column];
        return entry;
    }

    const std::optional<T>& GetEntry(std::size_t index) const
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        return entry;
    }

    std::optional<T>& GetEntry(std::size_t index)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        return entry;
    }

    T& Get(std::size_t index)
    {
        auto& entry = GetEntry(index);
        if (!entry)
        {
            Panic("RotatingVector: Get failed");
        }
        return *entry;
    }

    const T& Get(std::size_t index) const
    {
        const auto& entry = GetEntry(index);
        if (!entry)
        {
            Panic("RotatingVector: Get failed");
        }
        return *entry;
    }

    void Delete(std::size_t index)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        entry = std::nullopt;
        valid = false;
    }

    void DeleteWithoutDeallocating(std::size_t index)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        // entry = std::nullopt;
        valid = false;
    }

    bool empty() const
    {
        return false;
    }

private:
    std::array<std::array<RotatingVectorEntry<std::optional<T>>, N>, ROWS> data;
};

template<typename T, std::size_t N = 1, std::size_t ROWS = 4>
class RotatingVector
{
public:
    explicit RotatingVector()
    {
        static_assert(N > 0, "N must be greater than 0");
        static_assert(ROWS > 0, "ROWS must be greater than 0");

        for (auto& row : data)
        {
            row.fill(std::nullopt);
        }
    }

    inline RowColumnIndex GetRowColumnIndex(std::size_t index)
    {
        return RowColumnIndex{(index / N) % ROWS, index % N};
    }

    bool Insert(std::size_t index, T&& value)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& entry = data[row][column];
        if (entry)
        {
            return false;
        }
        entry = std::forward<T>(value);
        return true;
    }

    bool InsertUnsafe(std::size_t index, const T& value)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& entry = data[row][column];
        if (entry)
        {
            return false;
        }
        entry = std::move(value);
        return true;
    }

    const std::optional<T>& GetEntry(std::size_t index) const
    {
        auto [row, column] = GetRowColumnIndex(index);
        return data[row][column];
    }

    std::optional<T>& GetEntry(std::size_t index)
    {
        auto [row, column] = GetRowColumnIndex(index);
        return data[row][column];
    }

    T& Get(std::size_t index)
    {
        return *GetEntry(index);
    }

    void Delete(std::size_t index)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& entry = data[row][column];
        entry = std::nullopt;
    }

private:
    std::array<std::array<std::optional<T>, N>, ROWS> data;
};
