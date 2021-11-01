#pragma once

#include <faabric/util/locks.h>

#include <parallel_hashmap/phmap.h>

namespace faabric::util {

// See documentation for phmap: https://greg7mdp.github.io/parallel-hashmap/
// Specifically section titled: "Using the intrinsic parallelism ..."

template<typename K, typename V>
using SimpleMap =
  phmap::parallel_flat_hash_map<K,
                                V,
                                phmap::priv::hash_default_hash<K>,
                                phmap::priv::hash_default_eq<K>,
                                std::allocator<std::pair<const K, V>>,
                                4>;

template<typename K, typename V>
using ParallelMap =
  phmap::parallel_flat_hash_map<K,
                                V,
                                phmap::priv::hash_default_hash<K>,
                                phmap::priv::hash_default_eq<K>,
                                std::allocator<std::pair<const K, V>>,
                                4,
                                std::mutex>;

template<typename K, typename V>
class SlowParallelMap
{
  public:
    V& at(const K& key) { return map.at(key); }

    V& getOrInsert(const K& key)
    {
        getOrInsert(key, [key](std::unordered_map<K, V>& m) { m[key]; });
    }

    V& getOrInsert(
      const K& key,
      const std::function<void(std::unordered_map<K, V>)>& insertOp)
    {
        if (map.find(key) == map.end()) {
            FullLock lock(mx);
            if (map.find(key) == map.end()) {
                insertOp();
            }
        }

        {
            SharedLock lock(mx);
            return map.at(key);
        }
    }

    void insert(const K& key, const V& value) { map.insert(key, value); }

    void erase(const K& key) { map.erase(key); }

    void clear() { map.clear(); }

  private:
    std::shared_mutex mx;
    std::unordered_map<K, V> map;
};
}
