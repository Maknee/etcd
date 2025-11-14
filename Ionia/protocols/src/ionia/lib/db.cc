#include "db.h"

using namespace std;

//
//  splinter_db.cc
//  YCSB-C
//
//  Created by Rob Johnson on 3/20/2022.
//  Copyright (c) 2022 VMware.
//

extern "C" {
#include "splinterdb/default_data_config.h"
}

#include <string>
#include <vector>

using std::string;
using std::vector;

InMemoryDB::InMemoryDB(Properties &props, bool preloaded)
{
}

InMemoryDB::~InMemoryDB()
{
}

void InMemoryDB::Init()
{
}

void InMemoryDB::Close()
{
}

int InMemoryDB::Read(const string &table,
                     const string &key,
                     const vector<string> *fields,
                     vector<KVPair> &result) {
  if (auto found = kv_store.find(key); found != std::end(kv_store))
  {
    result.emplace_back(KVPair{key, found->second});
  }
  return DB::kOK;
}

int InMemoryDB::Scan(const string &table,
                     const string &key, int len,
                     const vector<string> *fields,
                     vector<vector<KVPair>> &result) {
  Panic("Scan not implemented");
  return DB::kOK;
}

int InMemoryDB::Update(const string &table,
                       const string &key,
                       vector<KVPair> &values) {
  return Insert(table, key, values);
}

int InMemoryDB::Insert(const string &table, const string &key, vector<KVPair> &values) {
  assert(values.size() == 1);
  std::string value = values[0].second;

  kv_store.lazy_emplace_l(key,
    [&](auto& v) { v.second = value; },
    [&](const auto& ctor) { ctor(key, value); }
  );
  return DB::kOK;
}

int InMemoryDB::Delete(const string &table, const string &key) {
  Panic("Delete not implemented");
  return DB::kOK;
}

SplinterDB::SplinterDB(Properties &props, bool preloaded) {
  uint64_t max_key_size = props.GetIntProperty("splinterdb.max_key_size");

  default_data_config_init(max_key_size, &data_cfg);
  splinterdb_cfg.filename                 = props.GetProperty("splinterdb.filename").c_str();
  splinterdb_cfg.cache_size               = props.GetIntProperty("splinterdb.cache_size_mb") * 1024 *1024;
  splinterdb_cfg.disk_size                = props.GetIntProperty("splinterdb.disk_size_gb") * 1024 * 1024 * 1024;
  splinterdb_cfg.data_cfg                 = &data_cfg;
  splinterdb_cfg.heap_handle              = NULL;
  splinterdb_cfg.heap_id                  = NULL;
  splinterdb_cfg.page_size                = props.GetIntProperty("splinterdb.page_size");
  splinterdb_cfg.extent_size              = props.GetIntProperty("splinterdb.extent_size");
  splinterdb_cfg.io_flags                 = props.GetIntProperty("splinterdb.io_flags");
  splinterdb_cfg.io_perms                 = props.GetIntProperty("splinterdb.io_perms");
  splinterdb_cfg.io_async_queue_depth     = props.GetIntProperty("splinterdb.io_async_queue_depth");
  splinterdb_cfg.cache_use_stats          = props.GetIntProperty("splinterdb.cache_use_stats");
  splinterdb_cfg.cache_logfile            = props.GetProperty("splinterdb.cache_logfile").c_str();
  splinterdb_cfg.btree_rough_count_height = props.GetIntProperty("splinterdb.btree_rough_count_height");
  splinterdb_cfg.filter_remainder_size    = props.GetIntProperty("splinterdb.filter_remainder_size");
  splinterdb_cfg.filter_index_size        = props.GetIntProperty("splinterdb.filter_index_size");
  splinterdb_cfg.use_log                  = props.GetIntProperty("splinterdb.use_log");
  splinterdb_cfg.memtable_capacity        = props.GetIntProperty("splinterdb.memtable_capacity");
  splinterdb_cfg.fanout                   = props.GetIntProperty("splinterdb.fanout");
  splinterdb_cfg.max_branches_per_node    = props.GetIntProperty("splinterdb.max_branches_per_node");
  splinterdb_cfg.use_stats                = props.GetIntProperty("splinterdb.use_stats");
  splinterdb_cfg.reclaim_threshold        = props.GetIntProperty("splinterdb.reclaim_threshold");

  if (preloaded) {
    assert(!splinterdb_open(&splinterdb_cfg, &spl));
  } else {
    assert(!splinterdb_create(&splinterdb_cfg, &spl));
  }
}

SplinterDB::~SplinterDB()
{
  if (spl)
  {
    splinterdb_close(&spl);
  }
}

void SplinterDB::Init()
{
  splinterdb_register_thread(spl);
}

void SplinterDB::Close()
{
  splinterdb_deregister_thread(spl);
}

void SplinterDB::Shutdown()
{
  if (spl)
  {
    splinterdb_close(&spl);
  }
}

int SplinterDB::Read(const string &table,
                     const string &key,
                     const vector<string> *fields,
                     vector<KVPair> &result) {
  splinterdb_lookup_result  lookup_result;
  splinterdb_lookup_result_init(spl, &lookup_result, 0, NULL);
  slice key_slice = slice_create(key.size(), key.c_str());
  //cout << "lookup " << key << endl;
  assert(!splinterdb_lookup(spl, key_slice, &lookup_result));
  if (!splinterdb_lookup_found(&lookup_result)) {
    cout << "FAILED lookup " << key << endl;
    assert(0);
  }

        slice result_value;
        int rc = splinterdb_lookup_result_value(&lookup_result, &result_value);

        /*
        if (result_value.data)
        {
                //Notice("ret value = %s", ret_value);
        }
        if(!rc){
      printf("Found key: '%s', value: '%.*s'\n",
             key.c_str(),
             (int)slice_length(result_value),
             (char *)slice_data(result_value));
        }
        */


  //cout << "done lookup " << key << endl;
  splinterdb_lookup_result_deinit(&lookup_result);
  return DB::kOK;
}

struct SplinterLookUpResult
{
  SplinterLookUpResult(splinterdb* spl) : spl(spl) {
    splinterdb_lookup_result_init(spl, &lookup_result, 0, NULL);
  }
  ~SplinterLookUpResult() {
    splinterdb_lookup_result_deinit(&lookup_result);
  }
  splinterdb* spl;
  splinterdb_lookup_result lookup_result;
};

std::optional<std::string> SplinterDB::Read(const std::string &table, std::string_view key)
{
  SplinterLookUpResult lookup_result_(spl);
  splinterdb_lookup_result& lookup_result = lookup_result_.lookup_result;

  // Warning("LOOKUP %.*s", (int)key.length(), key.data());
  slice key_slice = slice_create(key.length(), key.data());
  if (splinterdb_lookup(spl, key_slice, &lookup_result))
  {
    return std::nullopt;
  }
  if (!splinterdb_lookup_found(&lookup_result))
  {
    return std::nullopt;
  }
  slice result_value;
  if (splinterdb_lookup_result_value(&lookup_result, &result_value))
  {
    return std::nullopt;
  }

  return std::string((char*)slice_data(result_value), slice_length(result_value));
}

int SplinterDB::Scan(const string &table,
                     const string &key, int len,
                     const vector<string> *fields,
                     vector<vector<KVPair>> &result) {
  assert(fields == NULL);

  slice key_slice = slice_create(key.size(), key.c_str());

  splinterdb_iterator *itor;
  assert(!splinterdb_iterator_init(spl, &itor, key_slice));
  for (int i = 0; i < len; i++) {
    if (!splinterdb_iterator_valid(itor)) {
      break;
    }
    slice key, val;
    splinterdb_iterator_get_current(itor, &key, &val);
    splinterdb_iterator_next(itor);
  }
  assert(!splinterdb_iterator_status(itor));
  splinterdb_iterator_deinit(itor);

  return DB::kOK;
}

int SplinterDB::Update(const string &table,
                       const string &key,
                       vector<KVPair> &values) {
  return Insert(table, key, values);
}

int SplinterDB::Insert(const string &table, const string &key, vector<KVPair> &values) {
  assert(values.size() == 1);

  std::string val = values[0].second;
  slice key_slice = slice_create(key.size(), key.c_str());
  slice val_slice = slice_create(val.size(), val.c_str());
  //cout << "insert " << key << endl;
  assert(!splinterdb_insert(spl, key_slice, val_slice));
  //cout << "done insert " << key << endl;
  num_entries++;
  
  return DB::kOK;
}

int SplinterDB::Insert(const std::string& table, std::string_view key, std::string_view value)
{
  slice key_slice = slice_create(key.length(), key.data());
  slice val_slice = slice_create(value.length(), value.data());
  // Warning("insert %.*s", (int)key.length(), key.data());
  if (splinterdb_insert(spl, key_slice, val_slice))
  {
    return DB::kErrorNoData;
  }
  num_entries++;
  return DB::kOK;
}


int SplinterDB::Delete(const string &table, const string &key)
{
  slice key_slice = slice_create(key.size(), key.c_str());
  assert(!splinterdb_delete(spl, key_slice));

  return DB::kOK;
}

std::size_t SplinterDB::Size() const
{
  // Taken from https://github.com/vmware/splinterdb/blob/main/examples/splinterdb_iterators_example.c#L138
  const char *from_key = NULL;
  splinterdb_iterator *it = NULL;

  // -- ACTION IS HERE --
  // Initialize start key if initial search key was provided.
  slice start_key =
    (from_key ? slice_create(strlen(from_key), from_key) : NULL_SLICE);
  int rc = splinterdb_iterator_init(spl, &it, start_key);

  int i = 0;

  for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
    slice key, value;
    splinterdb_iterator_get_current(it, &key, &value);
    // printf("[%d] key='%.*s', value='%.*s'\n",
    //         i,
    //         (int)slice_length(key),
    //         (char *)slice_data(key),
    //         (int)slice_length(value),
    //         (char *)slice_data(value));
    i++;
  }
  rc = splinterdb_iterator_status(it);
  splinterdb_iterator_deinit(it);

  return i;
}