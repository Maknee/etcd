#include <stdint.h>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <iostream>
#include <cassert>

#include "batchgen.h"

using namespace std;

void Batch_Gen::initBatch(size_t batch_sz, size_t thread_ct) {
  max_trace_size = batch_sz;
  thread_count = thread_ct;

  trace = new vector<uint64_t>[thread_count];
  
  queue = reinterpret_cast<boost::lockfree::spsc_queue<uint64_t>**>
	  (malloc(sizeof(boost::lockfree::spsc_queue<uint64_t>*) * thread_count));
  for(int i = 0; i<thread_count; i++)
  {
    queue[i] = new boost::lockfree::spsc_queue<uint64_t>{100000};
  }

  tracebuf = reinterpret_cast<buf_t*>(malloc(sizeof(buf_t) * thread_count));
  for(int i = 0; i<thread_count; i++){
    assert(buf_init(&tracebuf[i], 0) != NULL); 
  }
}


vector<uint64_t> Batch_Gen::strToInt(vector<string> str) {
  vector<uint64_t> vect;

  for(string s:str)
    vect.push_back(hashStr(reinterpret_cast<const unsigned char *>(s.c_str())));

  return vect;
}


uint64_t Batch_Gen::hashStr(unsigned const char *str) {
    uint64_t hash = 5381;
    int c;
    while (*str != '\0') {
        c = *str;
        hash = ((hash << 5) + hash) + c;
        str++;
    }
    return hash;
}


vector<uint64_t> Batch_Gen::findDuplicate(vector<uint64_t>& nums) {
  unordered_set<int> hset;
  vector<uint64_t> duplicate_index;
  for(uint64_t idx = 0; idx < nums.size(); idx++) {
    if(hset.count(nums[idx])){
      duplicate_index.push_back(idx);
    }
    hset.insert(nums[idx]);
  }
  return duplicate_index;
}


vector<uint64_t> Batch_Gen::findDupQueue(vector<uint64_t>& dups, vector<uint64_t>& vect) {
  int i = dups.size()-1;
  vector<uint64_t> one_key_vect;
  for (int j = 0; j <vect.size(); j++){
    if(vect[j] == vect[dups[dups.size()-1]])
      one_key_vect.push_back(j);
  }
  dups.erase(dups.end()-1);
  return one_key_vect;
}

// by getting the op index x,
// we find (x, y] range that op fall into
/*
opnum_t Batch_Gen::getBeginningOpindex(size_t threadid, opnum_t op){
  if(batch_divider[threadid].size() == 0)
    return -2;
  if(batch_divider[threadid][1]>=op)
    return 0; 
  for (int i = 1; i<batch_divider[threadid].size()-1; i++){
    if(batch_divider[threadid][i+1]>=op){
      assert(batch_divider[threadid][i]<op);
      return i;
    }
  }

  return -1;
}

opnum_t Batch_Gen::getIndex(size_t threadid, opnum_t op){
  int base_index = getBeginningOpindex(threadid, op);
  if(base_index < 0)
    return -1;
  if(batch_size[threadid].size() == 0)
    return -2; //no pending op in the trace
  opnum_t begin_op = batch_divider[threadid][base_index];
  opnum_t op_index = begin_op + batch_size[threadid][base_index] -1 ;
  return op_index;
}

*/

void Batch_Gen::popTrace(size_t threadid){
  trace[(int)threadid].erase(trace[(int)threadid].begin());
}


uint64_t Batch_Gen::getAndDeleteFromTrace(size_t threadid){
  //while(!queue[threadid]->pop(index));
  buf_entry* bet = buf_pull(&tracebuf[threadid]);
  return bet->index;
}


void Batch_Gen::generateOpTrace(vector<string> vect, opnum_t begin, opnum_t end){
  assert(vect.size() == (end-begin));

  vector<opnum_t> tmp;
  for(int i = begin; i<end; i++)
    tmp.push_back(i);

  for(int i = 0; i<vect.size(); i++){
    uint64_t hashVal = hashStr(reinterpret_cast<const unsigned char *>(vect[i].c_str()));
    int tid = hashVal%thread_count;

    buf_push(&tracebuf[tid], tmp[i]);
    //trace[tid].push_back(tmp[i]);
    //while(!queue[tid]->push(tmp[i]));
  }
}

uint64_t Batch_Gen::getTraceSize(size_t threadid){
  return tracebuf[threadid].current_size;
}

//void Batch_Gen::getOneop(vector<uint64_t>& vect){
  
