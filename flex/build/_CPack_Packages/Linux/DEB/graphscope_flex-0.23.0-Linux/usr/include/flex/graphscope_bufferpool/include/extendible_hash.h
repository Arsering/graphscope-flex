/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "hash_table.h"

namespace gbp {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
  struct Bucket {
    Bucket(int depth) : localDepth(depth){};
    int localDepth;
    std::map<K, V> kmap;
    std::mutex latch;
  };

 public:
  // constructor
  ExtendibleHash(size_t size);
  ExtendibleHash();
  // helper function to generate hash addressing
  size_t HashKey(const K& key) const;
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K& key, V& value) override;
  bool Remove(const K& key) override;
  void Insert(const K& key, const V& value) override;

  int getIdx(const K& key) const;

 private:
  // add your own member variables here
  int globalDepth_;
  size_t bucketSize_;
  int bucketNum_;
  std::vector<std::shared_ptr<Bucket>> buckets_;
  // TODO: replace exclusive lock with rw lock
  mutable std::mutex latch_;
};
}  // namespace gbp
