// A circular extendable log.
// Each circular log is a pre-allocated 256MB chunk.
// Each insertion goes to the end of the used portion.
// When the log becomes full,
// we assume it is safe to overwrite the beginning:
// it's very likely that the beginning requests are
// already dispatched to the consensus replica.
//
// TODO: in the future, we can extend the circular log
// by allowing, instead of overwrite the beginning,
// we extend the circular log by allowing new chunks to join.
// [ 64MB ] -> [ 64MB ] -> [ 64MB ]

#include <sys/mman.h>
#include <errno.h>
#include <stdbool.h>

#include "vr/vr-proto.pb.h"
typedef std::pair<uint64_t, uint64_t> CXID;
typedef std::pair<uint64_t, specpaxos::vr::proto::RequestMessage> MsgPos;

#define typeof(x) __typeof__(x)
#define CAS_PTR(a, b, c)                                                \
    __extension__({                                                     \
        typeof(*a) _old = b, _new = c;                                  \
        __atomic_compare_exchange(a, &_old, &_new, 0, __ATOMIC_SEQ_CST, \
                                  __ATOMIC_SEQ_CST);                    \
        _old;                                                           \
    })


static const size_t MB = 1024 * 1024;
static const size_t DEFAULT_LOG_SIZE = 256; //default log size in mb
static const size_t DEFAULT_LOG_ENTRY_SIZE = sizeof(CXID) + sizeof(MsgPos); //default entry size in byte

// each node stores one request
typedef struct log_entry{
  uint8_t* data;
  size_t data_size;
  struct log_entry *next;
  int valid; //valid=log_t->valid_code stands for a valid entry
}log_entry;

typedef struct log_t{
  log_entry *log_head;
  log_entry *log_tail;
  log_entry *push_ptr;
  log_entry *pull_ptr;
  size_t current_size;
  size_t log_size;
  int valid_code;
}log_t;

static log_t* log_init(log_t* log, size_t log_size){
  if(log_size == 0)
    log_size = DEFAULT_LOG_SIZE * MB; 
  else
    log_size = log_size * MB;

  int prot  = PROT_READ | PROT_WRITE;
  int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;

  void* log_begin_addr = mmap(NULL, log_size, prot, flags, -1, 0);
  if (log_begin_addr == MAP_FAILED) {
    printf("mmap (%lu) failed with error: %s\n", log_size, strerror(errno));
    return NULL;
  }

  log->log_head = (log_entry*) log_begin_addr;
  log->log_tail = (log_entry*) ((uint64_t)log_begin_addr + log_size);

  log->push_ptr = log->log_head;
  log->pull_ptr = log->log_head;

  log->log_size = log_size;
  log->current_size = 0;
  log->valid_code = 1;

  return log;
}


static log_entry* log_push(log_t *log, uint8_t* data, size_t size){
  while(1){
    log_entry *old_push_ptr = log->push_ptr;
    uint64_t cur_entry_end_addr = (uint64_t) old_push_ptr + size + sizeof(log_entry);
    log_entry *new_push_ptr = (log_entry*) cur_entry_end_addr;

    while((uint64_t)cur_entry_end_addr > (uint64_t)log->log_tail){
      //check if the log beginning is consumed enough
      //---- the log has to be at lease half empty
      // TODO: decide the behavior --- wait on someone consumes the log,
      // actively consume the log, or extend the log
      while(((uint64_t)(log->pull_ptr)-(uint64_t)(log->log_head)) > (log->log_size / 2)){
	if(CAS_PTR(&(log->push_ptr), old_push_ptr, log->log_head) == old_push_ptr){
	  old_push_ptr = log->push_ptr;
	  cur_entry_end_addr = (uint64_t) old_push_ptr + size + sizeof(log_entry);
	  new_push_ptr = (log_entry*) cur_entry_end_addr;
	  //TODO: do we need to increment valid code atomically?
	  log->valid_code++;
	  while(1){
	      size_t old_size = log->current_size;
	      size_t new_size = 0;
	      if(CAS_PTR(&(log->current_size), old_size, new_size) == old_size){
	        break;
	      }
	  }
	  break;
	}
      }
      /*
      else{
	assert(0);
	//TODO: extend the log
      }
      */



    }

    // when reusing the circular buffer,
    // if the accumulating rate is higher than consuming rate
    // we may risk at writing in unconsumed address
    if (((uint64_t)cur_entry_end_addr > (uint64_t)log->pull_ptr)
	&& ((uint64_t)log->pull_ptr > (uint64_t)log->push_ptr)){
      //TODO: extend the log
    }

    if(CAS_PTR(&(log->push_ptr), old_push_ptr, new_push_ptr) == old_push_ptr){

      uint8_t* data_addr = (uint8_t*) ((uint64_t)old_push_ptr + sizeof(log_entry));
      memcpy(data_addr, data, size);
      old_push_ptr->data_size = size;
      old_push_ptr->next = new_push_ptr;
      old_push_ptr->valid = log->valid_code;


      while(1){
	size_t old_size = log->current_size;
	size_t new_size = log->current_size + 1;
	if(CAS_PTR(&(log->current_size), old_size, new_size) == old_size){
          break;
	}
      }

      return old_push_ptr;
    }
  }
}

static int insert_or_assign(log_t *log, CXID cx, MsgPos msg){
  uint8_t* buf = (uint8_t*)malloc(sizeof(cx)+sizeof(msg));
  memcpy(buf, &cx, sizeof(cx));
  memcpy((void*)((uint64_t)buf+sizeof(cx)), &msg, sizeof(msg));

  void* ret = log_push(log, buf, sizeof(cx)+sizeof(msg));
  assert(ret != NULL);

  return 0;
}


static bool log_is_empty(log_t* log, size_t log_entry_size){
  if(log_entry_size == 0)
    log_entry_size = DEFAULT_LOG_ENTRY_SIZE;
  // this is the case when the log is accumulating before reuse
  //  pull_ptr      push_ptr
  //     |             |
  // BBBBBBBBBBBBBBBBBBB-----------------------------------------
  if((uint64_t)(log->push_ptr)>= (uint64_t)(log->pull_ptr)){
    log_entry *old_pull_ptr = log->pull_ptr;
    uint64_t cur_entry_end_addr = (uint64_t) old_pull_ptr + log_entry_size + sizeof(log_entry);
    if(cur_entry_end_addr >= (uint64_t)(log->push_ptr)){
      //TODO: do we need to check it atomically (with resp. to actual pulling)
      if(old_pull_ptr->valid != log->valid_code){
        return true;
      }
      else{
	return false;
      }
    }
    else{
      return false;
    }
  }

  // this is the case when the log is partially on reusing
  //             push_ptr                 pull_ptr 
  //                |                         |
  // BBBBBBBBBBBBBBBB--------AAAAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBB
  if((uint64_t)(log->push_ptr) < (uint64_t)(log->pull_ptr)){
    return false;
  }
}

static log_entry* log_pull(log_t *log, size_t size){
  if(size == 0)
    size = DEFAULT_LOG_ENTRY_SIZE;

  while(1){
    log_entry *old_pull_ptr = log->pull_ptr;
    uint64_t cur_entry_end_addr = (uint64_t) old_pull_ptr + size + sizeof(log_entry);
    log_entry *new_pull_ptr = (log_entry*) cur_entry_end_addr; 

    // check if the pull ptr has reached log tail
    // go back to log head if the pull ptr has reached the end
    while((uint64_t)cur_entry_end_addr > (uint64_t)log->log_tail){
      if(CAS_PTR(&(log->pull_ptr), old_pull_ptr, log->log_head) == old_pull_ptr){
        old_pull_ptr = log->pull_ptr;
        cur_entry_end_addr = (uint64_t) old_pull_ptr + size + sizeof(log_entry);
        new_pull_ptr = (log_entry*) cur_entry_end_addr;
        break;
      }
    }


    // check if the log has unpulled entry
    if(log_is_empty(log, size))
    {
      return NULL;
    }

    if(CAS_PTR(&(log->pull_ptr), old_pull_ptr, new_pull_ptr) == old_pull_ptr){
      // data stored at (void*) ((uint64_t)old_pull_ptr + sizeof(log_entry*))
      // data size stored at old_pull_ptr->data_size
      // next entry starts at old_pull_ptr->next or new_pull_ptr

      while(1){
        size_t old_size = log->current_size;
	size_t new_size = log->current_size - 1;
	if(new_size < 0)
	  break;
	if(CAS_PTR(&(log->current_size), old_size, new_size) == old_size){
	  break;
	}
      }

      return old_pull_ptr;
    }
  }
}


static void erase(log_t *log, CXID cx){
  void* ret = log_pull(log, sizeof(CXID)+sizeof(MsgPos));
  assert(ret != 0);
}


static void log_clear(log_t *log){
  //TODO: do we need to increment valid code atomically?
  log->valid_code++;
  log->current_size = 0;
}



