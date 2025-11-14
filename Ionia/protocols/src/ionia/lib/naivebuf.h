// A circular extendable buf.
// Each circular buf is a pre-allocated 256MB chunk.
// Each insertion goes to the end of the used portion.
// When the buf becomes full,
// we assume it is safe to overwrite the beginning:
// it's very likely that the beginning requests are
// already dispatched to the consensus replica.
//
// TODO: in the future, we can extend the circular buf
// by allowing, instead of overwrite the beginning,
// we extend the circular buf by allowing new chunks to join.
// [ 64MB ] -> [ 64MB ] -> [ 64MB ]

#include <sys/mman.h>
#include <errno.h>
#include <stdbool.h>
#include <cstdint>
#include <cstddef>
#include <cstdio>
#include <cstring>

#define typeof(x) __typeof__(x)
#define CAS_PTR(a, b, c)                                                \
    __extension__({                                                     \
        typeof(*a) _old = b, _new = c;                                  \
        __atomic_compare_exchange(a, &_old, &_new, 0, __ATOMIC_SEQ_CST, \
                                  __ATOMIC_SEQ_CST);                    \
        _old;                                                           \
    })

static const size_t BUF_MB = 1024 * 1024;
static const size_t DEFAULT_BUF_SIZE = 256; //default buf size in mb
static const size_t DEFAULT_BUF_ENTRY_SIZE = 128; //default entry size in byte

// each node stores one request
typedef struct buf_entry{
  uint64_t index;
  struct buf_entry *next;
  int valid; //valid=buf_t->valid_code stands for a valid entry
}buf_entry;

typedef struct buf_t{
  buf_entry *buf_head;
  buf_entry *buf_tail;
  buf_entry *push_ptr;
  buf_entry *pull_ptr;
  size_t buf_size;
  size_t current_size;
  int valid_code;
}buf_t;

static buf_t* buf_init(buf_t* buf, size_t buf_size){
  if(buf_size == 0)
    buf_size = DEFAULT_BUF_SIZE * BUF_MB; 
  else
    buf_size = buf_size * BUF_MB;

  int prot  = PROT_READ | PROT_WRITE;
  int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;

  void* buf_begin_addr = mmap(NULL, buf_size, prot, flags, -1, 0);
  if (buf_begin_addr == MAP_FAILED) {
    printf("mmap (%lu) failed with error: %s\n", buf_size, strerror(errno));
    return NULL;
  }


  buf->buf_head = (buf_entry*) buf_begin_addr;
  buf->buf_tail = (buf_entry*) ((uint64_t)buf_begin_addr + buf_size);

  buf->push_ptr = buf->buf_head;
  buf->pull_ptr = buf->buf_head;

  buf->buf_size = buf_size;
  buf->current_size = 0;
  buf->valid_code = 1;

  return buf;
}


static buf_entry* buf_push(buf_t *buf, uint64_t index){
  while(1){
    buf_entry *old_push_ptr = buf->push_ptr;
    uint64_t cur_entry_end_addr = (uint64_t) old_push_ptr + sizeof(buf_entry);
    buf_entry *new_push_ptr = (buf_entry*) cur_entry_end_addr;

    while((uint64_t)cur_entry_end_addr > (uint64_t)buf->buf_tail){
      //check if the buf beginning is consumed enough
      //---- the buf has to be at lease half empty
      // TODO: decide the behavior --- wait on someone consumes the buf,
      // actively consume the buf, or extend the buf
      while(((uint64_t)(buf->pull_ptr)-(uint64_t)(buf->buf_head)) > (buf->buf_size / 2)){
	if(CAS_PTR(&(buf->push_ptr), old_push_ptr, buf->buf_head) == old_push_ptr){
	  old_push_ptr = buf->push_ptr;
	  cur_entry_end_addr = (uint64_t) old_push_ptr + sizeof(buf_entry);
	  new_push_ptr = (buf_entry*) cur_entry_end_addr;
	  //TODO: do we need to increment valid code atomically?
	  buf->valid_code++;
      while(1){
        size_t old_size = buf->current_size;
        size_t new_size = 0;
        if(CAS_PTR(&(buf->current_size), old_size, new_size) == old_size){
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
	//TODO: extend the buf
      }
      */

    }

    // when reusing the circular buffer,
    // if the accumulating rate is higher than consuming rate
    // we may risk at writing in unconsumed address
    if (((uint64_t)cur_entry_end_addr > (uint64_t)buf->pull_ptr)
	&& ((uint64_t)buf->pull_ptr > (uint64_t)buf->push_ptr)){
      //TODO: extend the buf
    }

    if(CAS_PTR(&(buf->push_ptr), old_push_ptr, new_push_ptr) == old_push_ptr){
      old_push_ptr->index = index;
      old_push_ptr->next = new_push_ptr;
      old_push_ptr->valid = buf->valid_code;
      while(1){
        size_t old_size = buf->current_size;
        size_t new_size = buf->current_size + 1;
        if(CAS_PTR(&(buf->current_size), old_size, new_size) == old_size){
          break;
        }
      }

      return old_push_ptr;
    }
  }
}



static bool buf_is_empty(buf_t* buf){
  // this is the case when the buf is accumulating before reuse
  //  pull_ptr      push_ptr
  //     |             |
  // BBBBBBBBBBBBBBBBBBB-----------------------------------------
  if((uint64_t)(buf->push_ptr)>= (uint64_t)(buf->pull_ptr)){
    buf_entry *old_pull_ptr = buf->pull_ptr;
    uint64_t cur_entry_end_addr = (uint64_t) old_pull_ptr + sizeof(buf_entry);
    if(cur_entry_end_addr >= (uint64_t)(buf->push_ptr)){
      //TODO: do we need to check it atomically (with resp. to actual pulling)
      if(old_pull_ptr->valid != buf->valid_code){
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

  // this is the case when the buf is partially on reusing
  //             push_ptr                 pull_ptr 
  //                |                         |
  // BBBBBBBBBBBBBBBB--------AAAAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBB
  if((uint64_t)(buf->push_ptr) < (uint64_t)(buf->pull_ptr)){
    return false;
  }

  assert(0);
  return false;
}

static buf_entry* buf_pull(buf_t *buf){
  while(1){
    buf_entry *old_pull_ptr = buf->pull_ptr;
    uint64_t cur_entry_end_addr = (uint64_t) old_pull_ptr + sizeof(buf_entry);
    buf_entry *new_pull_ptr = (buf_entry*) cur_entry_end_addr; 

    // check if the pull ptr has reached buf tail
    // go back to buf head if the pull ptr has reached the end
    while((uint64_t)cur_entry_end_addr > (uint64_t)buf->buf_tail){
      if(CAS_PTR(&(buf->pull_ptr), old_pull_ptr, buf->buf_head) == old_pull_ptr){
        old_pull_ptr = buf->pull_ptr;
        cur_entry_end_addr = (uint64_t) old_pull_ptr + sizeof(buf_entry);
        new_pull_ptr = (buf_entry*) cur_entry_end_addr;
        break;
      }
    }


    // check if the buf has unpulled entry
    if(buf_is_empty(buf))
    {
      return NULL;
    }

    if(CAS_PTR(&(buf->pull_ptr), old_pull_ptr, new_pull_ptr) == old_pull_ptr){
      // data stored at (void*) ((uint64_t)old_pull_ptr + sizeof(buf_entry*))
      // data size stored at old_pull_ptr->data_size
      // next entry starts at old_pull_ptr->next or new_pull_ptr

      while(1){
        size_t old_size = buf->current_size;
	size_t new_size = buf->current_size - 1;
	if(CAS_PTR(&(buf->current_size), old_size, new_size) == old_size){
	  break;
	}
      }
      return old_pull_ptr;
    }
  }
}


static void buf_clear(buf_t *buf){
  //TODO: do we need to increment valid code atomically?
  buf->valid_code++;
  buf->current_size = 0;
}
