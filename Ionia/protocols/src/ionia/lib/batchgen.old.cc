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

  trace_prev_size = new uint64_t[thread_count];
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
  trace_prev_size[(int)threadid]--;
  trace[(int)threadid].erase(trace[(int)threadid].begin());
}


bool Batch_Gen::generateTrace(vector<uint64_t>& vect, opnum_t lastcommit){
  bool ret = true;
  if(vect.size()<1)
    return ret;

  for(int j = 0; j<thread_count; j++){
    //batch_divider[j].push_back(lastcommit+vect.size());
    //batch_size[j].push_back(0);
    trace_prev_size[j] = trace[j].size();
  }

  vector<uint64_t> dups = findDuplicate(vect);


  vector<bool> bagelem;
  for(int i = 0; i<vect.size(); i++){
    bagelem.push_back(true);
  }
  for(int i = 0; i<dups.size(); i++){
    bagelem[dups[i]] = false;
  }

  size_t min_thread_id=0;
  size_t dups_old_size = dups.size();
  int* dedup = new int[dups.size()];
  for(int i= 0; i<dups_old_size; i++){
    dedup[i] = -1;
  }
  int dedup_idx = 0;
  while(dups.size()>0){
next_key:
    if(dups.size() == 0)
      break;
    vector<uint64_t> one_key_vect = findDupQueue(dups, vect);
    for(int i = 0; i < dups_old_size; i++){
      if(dedup[i] == one_key_vect[0]){
        goto next_key;
      }
    }

    dedup[dedup_idx] = one_key_vect[0];
    bagelem[one_key_vect[0]] = false;
    dedup_idx++;
    bool insertion_suc = false;
    int tid = vect[one_key_vect[0]] %thread_count;
    if(max_trace_size-trace[tid].size()>=one_key_vect.size())
      insertion_suc = true;
    for(int k = 0; k<one_key_vect.size(); k++){
      trace[tid].push_back(one_key_vect[k]);
      //batch_size[tid][batch_size[tid].size()-1]++;
    }

    if(!insertion_suc)
      ret = false;
  }


  for(int k = 0; k<vect.size(); k++){
    if(bagelem[k]){
      bool insertion_suc = false;
      int tid = k%thread_count;
      if(trace[tid].size()+1<max_trace_size)
        insertion_suc = true;
      trace[tid].push_back(k);
      //batch_size[tid][batch_size[tid].size()-1]++;
      if(!insertion_suc){
	ret = false;
      }
    }
  }
finish:
  /*
  printf("printing out the trace \n");
  for(int i = 0; i<thread_count; i++)
  {
    for(int j = 0; j<trace[i].size(); j++)
      printf("%d ", trace[i][j]);
    printf("\n");
  }
  */

  return ret;
}

void Batch_Gen::generateOpTrace(opnum_t begin, opnum_t end){
  vector<opnum_t> tmp;
  for(int i = begin; i<end; i++)
    tmp.push_back(i);

  for(int i = 0; i<thread_count; i++)
  {
    if(begin == end){
      //FIXME: should not clear the whole trace[i], 
      //instead, only clear the trace_prev_size[i] to trace[i].size() portion
      trace[i].clear();
    }
    else{
      for(int j = trace_prev_size[i]; j<trace[i].size(); j++){
        trace[i][j] = tmp[trace[i][j]];
      }
    }
  }
}


//void Batch_Gen::getOneop(vector<uint64_t>& vect){
  
