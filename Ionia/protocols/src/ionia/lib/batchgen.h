#include <stdint.h>
#include <vector>
#include <unordered_set>

#include <boost/lockfree/spsc_queue.hpp>
#include "lib/viewstamp.h"
#include "lib/naivebuf.h"

using namespace std;

//typedef uint64_t opnum_t;

class Batch_Gen {
public:
  size_t max_trace_size;
  size_t thread_count;

  //uint64_t **exec_trace;
  //int *trace_ptr;

  vector<uint64_t> *trace;

  boost::lockfree::spsc_queue<uint64_t> **queue;

  buf_t *tracebuf;

  vector<uint64_t> *t;

  void initBatch(size_t batch_size, size_t thread_count);
  vector<uint64_t> findDuplicate(vector<uint64_t>& nums);
  uint64_t hashStr(unsigned const char *str);
  vector<uint64_t> strToInt(vector<string> str);

  vector<uint64_t> findDupQueue(vector<uint64_t>& dups, vector<uint64_t>& vect);

  bool generateTrace(vector<uint64_t>& vect, opnum_t lastcommit);
  void generateOpTrace(vector<string> str, opnum_t begin, opnum_t end);

  uint64_t getAndDeleteFromTrace(size_t threadid);

  opnum_t getBeginningOpindex(size_t threadid, opnum_t op);
  opnum_t getIndex(size_t threadid, opnum_t op);
  void popTrace(size_t threadid);

  uint64_t getTraceSize(size_t threadid);
};
