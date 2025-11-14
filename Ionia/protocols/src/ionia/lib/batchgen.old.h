#include <stdint.h>
#include <vector>
#include <unordered_set>

#include "lib/viewstamp.h"

using namespace std;

//typedef uint64_t opnum_t;

class Batch_Gen {
public:
  size_t max_trace_size;
  size_t thread_count;

  //uint64_t **exec_trace;
  //int *trace_ptr;

  vector<uint64_t> *trace;
  /*
  vector<uint64_t> *batch_divider;
  vector<size_t> *batch_size;
  */

  uint64_t *trace_prev_size;

  void initBatch(size_t batch_size, size_t thread_count);
  vector<uint64_t> findDuplicate(vector<uint64_t>& nums);
  uint64_t hashStr(unsigned const char *str);
  vector<uint64_t> strToInt(vector<string> str);

  vector<uint64_t> findDupQueue(vector<uint64_t>& dups, vector<uint64_t>& vect);

  bool generateTrace(vector<uint64_t>& vect, opnum_t lastcommit);
  void generateOpTrace(opnum_t begin, opnum_t end);

  opnum_t getBeginningOpindex(size_t threadid, opnum_t op);
  opnum_t getIndex(size_t threadid, opnum_t op);
  void popTrace(size_t threadid);
};
