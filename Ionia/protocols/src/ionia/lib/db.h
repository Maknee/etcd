// Taken from ycsb-c

#pragma once

#include <string>
#include <map>
#include <fstream>
#include <cassert>
#include <vector>
#include <algorithm>
#include <iostream>
#include <sstream>

#include "lib/message.h"
#include "lib/data_structure.h"

extern "C"
{
    #include "splinterdb/splinterdb.h"
}

inline std::string Trim(const std::string &str) {
  auto front = std::find_if_not(str.begin(), str.end(), [](int c){ return std::isspace(c); });
  return std::string(front, std::find_if_not(str.rbegin(), std::string::const_reverse_iterator(front),
      [](int c){ return std::isspace(c); }).base());
}

class Properties {
 public:
  const std::string &GetProperty(const std::string &key,
      const std::string &default_value = std::string()) const;
  const std::string &operator[](const std::string &key) const;

  long int GetIntProperty(const std::string &key) const;
  //int operator[](const std::string &key) const;

  const std::map<std::string, std::string> &properties() const;

  void SetProperty(const std::string &key, const std::string &value);
  bool Load(std::ifstream &input);
 private:
  std::map<std::string, std::string> properties_;
};

inline const std::string &Properties::GetProperty(const std::string &key,
    const std::string &default_value) const {
  std::map<std::string, std::string>::const_iterator it = properties_.find(key);
  if (properties_.end() == it) {
    return default_value;
  } else return it->second;
}

inline const std::string &Properties::operator[](const std::string &key) const {
  return properties_.at(key);
}

inline long int Properties::GetIntProperty(const std::string &key) const {
  std::string s = GetProperty(key);
  return stol(s);
}

// inline int Properties::operator[](const std::string &key) const {
//   return GetProperty(key);
// }

inline const std::map<std::string, std::string> &Properties::properties() const {
  return properties_;
}

inline void Properties::SetProperty(const std::string &key,
                                    const std::string &value) {
  properties_[key] = value;
}

inline bool Properties::Load(std::ifstream &input) {
  if (!input.is_open()) return false;

  while (!input.eof() && !input.bad()) {
    std::string line;
    std::getline(input, line);
    if (line[0] == '#') continue;
    size_t pos = line.find_first_of('=');
    if (pos == std::string::npos) continue;
    SetProperty(Trim(line.substr(0, pos)), Trim(line.substr(pos + 1)));
  }
  return true;
}

class DB {
 public:
  typedef std::pair<std::string, std::string> KVPair;
  static const int kOK = 0;
  static const int kErrorNoData = 1;
  static const int kErrorConflict = 2;
  ///
  /// Initializes any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance globally.
  ///
  virtual void Init() { }
  ///
  /// Clears any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance globally.
  ///
  virtual void Close() { }
  virtual void Shutdown() {}
  ///
  /// Reads a record from the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of field/value pairs for the result.
  /// @return Zero on success, or a non-zero error code on error/record-miss.
  ///
  virtual int Read(const std::string &table, const std::string &key,
                   const std::vector<std::string> *fields,
                   std::vector<KVPair> &result) = 0;

  virtual std::optional<std::string> Read(const std::string &table, std::string_view key)
  {
    std::vector<KVPair> result;
    int ret = Read(table, std::string(key), nullptr, result);
    if (ret == kOK) {
      return result[0].second;
    } else {
      return std::nullopt;
    }
  }
  ///
  /// Performs a range scan for a set of records in the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param key The key of the first record to read.
  /// @param record_count The number of records to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of vector, where each vector contains field/value
  ///        pairs for one record
  /// @return Zero on success, or a non-zero error code on error.
  ///
  virtual int Scan(const std::string &table, const std::string &key,
                   int record_count, const std::vector<std::string> *fields,
                   std::vector<std::vector<KVPair>> &result) = 0;
  ///
  /// Updates a record in the database.
  /// Field/value pairs in the specified vector are written to the record,
  /// overwriting any existing values with the same field names.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to write.
  /// @param values A vector of field/value pairs to update in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual int Update(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) = 0;
  ///
  /// Inserts a record into the database.
  /// Field/value pairs in the specified vector are written into the record.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to insert.
  /// @param values A vector of field/value pairs to insert in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual int Insert(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) = 0;
  virtual int Insert(const std::string& table, std::string_view key, std::string_view value)
  {
    std::vector<KVPair> values;
    values.emplace_back(key, value);
    return Insert(table, std::string(key), values);
  }
  ///
  /// Deletes a record from the database.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to delete.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual int Delete(const std::string &table, const std::string &key) = 0;

  virtual std::size_t Size() const = 0;
  virtual std::size_t ApproxSize() const = 0;

  virtual ~DB() { }
};

class InMemoryDB : public DB {
public:
  InMemoryDB(Properties &props, bool preloaded);
  ~InMemoryDB();

  void Init();
  void Close();

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(const std::string &table, const std::string &key);
  std::size_t Size() const { return kv_store.size(); }
  std::size_t ApproxSize() const { return Size(); }

private:
  ThreadSafeMap<std::string, std::string> kv_store;
};

class SplinterDB : public DB {
public:
  SplinterDB(Properties &props, bool preloaded);
  ~SplinterDB();

  void Init();
  void Close();
  void Shutdown();

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);
  virtual std::optional<std::string> Read(const std::string &table, std::string_view key);

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);
  virtual int Insert(const std::string& table, std::string_view key, std::string_view value);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(const std::string &table, const std::string &key);
  std::size_t Size() const;
  std::size_t ApproxSize() const { return num_entries; }

private:
  splinterdb_config         splinterdb_cfg{};
  data_config               data_cfg{};
  splinterdb               *spl;
  std::size_t               num_entries{};
};
