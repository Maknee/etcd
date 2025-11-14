// A circular extendable queue.
// Each circular queue is a pre-allocated 256MB chunk.
// Each insertion goes to the end of the used portion.
// When the queue becomes full,
// we assume it is safe to overwrite the beginning:
// it's very likely that the beginning requests are
// already dispatched to the consensus replica.
//
// TODO: in the future, we can extend the circular queue
// by allowing, instead of overwrite the beginning,
// we extend the circular queue by allowing new chunks to join.
// [ 64MB ] -> [ 64MB ] -> [ 64MB ]

#include <sys/mman.h>
#include <errno.h>
#include <stdbool.h>
#include <cstdint>
#include <cstddef>
#include <cstdio>
#include <cstring>

#include "vr/vr-proto.pb.h"

#define typeof(x) __typeof__(x)
#define CAS_PTR(a, b, c)                                                \
    __extension__({                                                     \
        typeof(*a) _old = b, _new = c;                                  \
        __atomic_compare_exchange(a, &_old, &_new, 0, __ATOMIC_SEQ_CST, \
                                  __ATOMIC_SEQ_CST);                    \
        _old;                                                           \
    })

static const size_t QUEUE_MB = 1024 * 1024;
static const size_t DEFAULT_QUEUE_SIZE = 256; //default queue size in mb
static const size_t DEFAULT_QUEUE_ENTRY_SIZE = 128; //default entry size in byte

// each node stores one request
typedef struct queue_entry{
  specpaxos::vr::proto::RequestMessage msg;
  struct queue_entry *next;
  int valid; //valid=queue_t->valid_code stands for a valid entry
}queue_entry;

typedef struct queue_t{
  queue_entry *queue_head;
  queue_entry *queue_tail;
  queue_entry *push_ptr;
  queue_entry *pull_ptr;
  size_t queue_size;
  size_t current_size;
  int valid_code;
}queue_t;

static queue_t* queue_init(queue_t* queue, size_t queue_size){
  if(queue_size == 0)
    queue_size = DEFAULT_QUEUE_SIZE * QUEUE_MB; 
  else
    queue_size = queue_size * QUEUE_MB;

  int prot  = PROT_READ | PROT_WRITE;
  int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;

  void* queue_begin_addr = mmap(NULL, queue_size, prot, flags, -1, 0);
  if (queue_begin_addr == MAP_FAILED) {
    printf("mmap (%lu) failed with error: %s\n", queue_size, strerror(errno));
    return NULL;
  }


  queue->queue_head = (queue_entry*) queue_begin_addr;
  queue->queue_tail = (queue_entry*) ((uint64_t)queue_begin_addr + queue_size);

  queue->push_ptr = queue->queue_head;
  queue->pull_ptr = queue->queue_head;

  queue->queue_size = queue_size;
  queue->current_size = 0;
  queue->valid_code = 1;

  return queue;
}


static queue_entry* queue_push(queue_t *queue, specpaxos::vr::proto::RequestMessage msg){
  while(1){
    queue_entry *old_push_ptr = queue->push_ptr;
    uint64_t cur_entry_end_addr = (uint64_t) old_push_ptr + sizeof(queue_entry);
    queue_entry *new_push_ptr = (queue_entry*) cur_entry_end_addr;

    while((uint64_t)cur_entry_end_addr > (uint64_t)queue->queue_tail){
      //check if the queue beginning is consumed enough
      //---- the queue has to be at lease half empty
      // TODO: decide the behavior --- wait on someone consumes the queue,
      // actively consume the queue, or extend the queue
      while(((uint64_t)(queue->pull_ptr)-(uint64_t)(queue->queue_head)) > (queue->queue_size / 2)){
	if(CAS_PTR(&(queue->push_ptr), old_push_ptr, queue->queue_head) == old_push_ptr){
	  old_push_ptr = queue->push_ptr;
	  cur_entry_end_addr = (uint64_t) old_push_ptr + sizeof(queue_entry);
	  new_push_ptr = (queue_entry*) cur_entry_end_addr;
	  //TODO: do we need to increment valid code atomically?
	  queue->valid_code++;
      while(1){
        size_t old_size = queue->current_size;
        size_t new_size = 0;
        if(CAS_PTR(&(queue->current_size), old_size, new_size) == old_size){
          break;
        }
      }


	  break;
	}
      }
      /*
      else{
	printf("======================== Log consuming too slow =========================\n");
	assert(0);
	//TODO: extend the queue
      }
      */

    }

    // when reusing the circular queuefer,
    // if the accumulating rate is higher than consuming rate
    // we may risk at writing in unconsumed address
    if (((uint64_t)cur_entry_end_addr > (uint64_t)queue->pull_ptr)
	&& ((uint64_t)queue->pull_ptr > (uint64_t)queue->push_ptr)){
      //TODO: extend the queue
    }

    if(CAS_PTR(&(queue->push_ptr), old_push_ptr, new_push_ptr) == old_push_ptr){
      old_push_ptr->msg = msg;
      old_push_ptr->next = new_push_ptr;
      old_push_ptr->valid = queue->valid_code;
      while(1){
        size_t old_size = queue->current_size;
        size_t new_size = queue->current_size + 1;
        if(CAS_PTR(&(queue->current_size), old_size, new_size) == old_size){
          break;
        }
      }

      return old_push_ptr;
    }
  }
}



static bool queue_is_empty(queue_t* queue){
  // this is the case when the queue is accumulating before reuse
  //  pull_ptr      push_ptr
  //     |             |
  // BBBBBBBBBBBBBBBBBBB-----------------------------------------
  if((uint64_t)(queue->push_ptr)>= (uint64_t)(queue->pull_ptr)){
    queue_entry *old_pull_ptr = queue->pull_ptr;
    uint64_t cur_entry_end_addr = (uint64_t) old_pull_ptr + sizeof(queue_entry);
    if(cur_entry_end_addr >= (uint64_t)(queue->push_ptr)){
      //TODO: do we need to check it atomically (with resp. to actual pulling)
      if(old_pull_ptr->valid != queue->valid_code){
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

  // this is the case when the queue is partially on reusing
  //             push_ptr                 pull_ptr 
  //                |                         |
  // BBBBBBBBBBBBBBBB--------AAAAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBB
  if((uint64_t)(queue->push_ptr) < (uint64_t)(queue->pull_ptr)){
    return false;
  }

  assert(0);
  return false;
}

static queue_entry* queue_pull(queue_t *queue){
  while(1){
    queue_entry *old_pull_ptr = queue->pull_ptr;
    uint64_t cur_entry_end_addr = (uint64_t) old_pull_ptr + sizeof(queue_entry);
    queue_entry *new_pull_ptr = (queue_entry*) cur_entry_end_addr; 

    // check if the pull ptr has reached queue tail
    // go back to queue head if the pull ptr has reached the end
    while((uint64_t)cur_entry_end_addr > (uint64_t)queue->queue_tail){
      if(CAS_PTR(&(queue->pull_ptr), old_pull_ptr, queue->queue_head) == old_pull_ptr){
        old_pull_ptr = queue->pull_ptr;
        cur_entry_end_addr = (uint64_t) old_pull_ptr + sizeof(queue_entry);
        new_pull_ptr = (queue_entry*) cur_entry_end_addr;
        break;
      }
    }


    // check if the queue has unpulled entry
    if(queue_is_empty(queue))
    {
      return NULL;
    }

    if(CAS_PTR(&(queue->pull_ptr), old_pull_ptr, new_pull_ptr) == old_pull_ptr){
      // data stored at (void*) ((uint64_t)old_pull_ptr + sizeof(queue_entry*))
      // data size stored at old_pull_ptr->data_size
      // next entry starts at old_pull_ptr->next or new_pull_ptr

      while(1){
        size_t old_size = queue->current_size;
	size_t new_size = queue->current_size - 1;
	if(CAS_PTR(&(queue->current_size), old_size, new_size) == old_size){
	  break;
	}
      }
      return old_pull_ptr;
    }
  }
}


static void queue_clear(queue_t *queue){
  //TODO: do we need to increment valid code atomically?
  queue->valid_code++;
  queue->current_size = 0;
}
