#pragma once

#include <absl/container/flat_hash_map.h>
#include <boost/type_traits.hpp>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <tuple>
#include <type_traits>
#include <utility>

#include <faabric/util/locks.h>

namespace faabric::util {

namespace detail {

template<typename>
struct is_shared_ptr : std::false_type
{};

template<typename Pointee>
struct is_shared_ptr<std::shared_ptr<Pointee>> : std::true_type
{};

}

/*
 * A thread-safe wrapper around a hashmap
 *
 * Supports heterogeneous lookup, e.g. Key==std::string, lookup with
 * std::string_view
 *
 * Most such maps in faasm/faabric don't need to scale to many writers, so for
 * simplicity a shared_mutex is simply used instead of a more sophisticated
 * lock-free structure, but the underlying map could be swapped in the future if
 * needed.
 */
template<class Key, class Value>
class ConcurrentMap final
{
  private:
    mutable std::shared_mutex mutex;
    using UnderlyingMap = typename absl::flat_hash_map<Key, Value>;
    UnderlyingMap map;

    // Helper for heterogeneous lookup
    template<class T>
    using KeyArg = typename UnderlyingMap::template key_arg<T>;

  public:
    // Member types compatible with std containers
    using key_type = typename UnderlyingMap::key_type;
    using mapped_type = typename UnderlyingMap::value_type;
    using size_type = typename UnderlyingMap::size_type;
    using difference_type = typename UnderlyingMap::difference_type;

    ConcurrentMap() = default;
    ConcurrentMap(size_t initialCapacity)
      : map(initialCapacity)
    {}

    // Non-copyable but moveable
    ConcurrentMap(const ConcurrentMap&) = delete;
    ConcurrentMap& operator=(const ConcurrentMap&) = delete;
    ConcurrentMap(ConcurrentMap&&) = default;
    ConcurrentMap& operator=(ConcurrentMap&&) = default;
    void swap(ConcurrentMap<Key, Value>& other)
    {
        // Use the ADL swap idiom.
        using std::swap;
        swap(mutex, other.mutex);
        swap(map, other.map);
    }

    // Std-like capacity accessors
    bool isEmpty() const
    {
        SharedLock lock{ mutex };
        return map.empty();
    }

    size_t size() const
    {
        SharedLock lock{ mutex };
        return map.size();
    }

    size_t capacity() const
    {
        SharedLock lock{ mutex };
        return map.capacity();
    }

    void reserve(size_t count)
    {
        FullLock lock{ mutex };
        map.reserve(count);
    }

    // Rehashes the `flat_hash_map`, setting the number of slots to be at least
    // the passed value. Pass 0 to force a simple rehash.
    void rehash(size_t count)
    {
        FullLock lock{ mutex };
        map.rehash(count);
    }

    // Removes all elements
    void clear()
    {
        FullLock lock{ mutex };
        return map.clear();
    }

    // Remove an element by key, allows heterogeneous lookup.
    // Returns whether the key was found
    template<class K = Key>
    bool erase(const KeyArg<K>& key)
    {
        FullLock lock{ mutex };
        return map.erase(key) != 0;
    }

    // Inserts a <Key, Value> pair into the map if there isn't already one.
    // Returns whether the insertion happened.
    template<class P = std::pair<Key, Value>>
    bool insert(P&& pair)
    {
        FullLock lock{ mutex };
        auto [it, inserted] = map.insert(std::forward<P>(pair));
        return inserted;
    }

    // Inserts a <Key, Value> pair into the map or overwrites a previous value.
    // Returns whether the insertion happened.
    template<class K = Key>
    bool insertOrAssign(K&& key, Value&& value)
    {
        FullLock lock{ mutex };
        auto [it, inserted] =
          map.insert_or_assign(std::forward<K>(key), std::move(value));
        return inserted;
    }

    // Constructs a <Key, Value(args)> pair in the map if there isn't already
    // one. Returns whether the insertion happened.
    template<class K = Key, class... Args>
    bool tryEmplace(K&& key, Args&&... args)
    {
        FullLock lock{ mutex };
        auto [it, inserted] =
          map.try_emplace(std::forward<K>(key), std::forward<Args>(args)...);
        return inserted;
    }

    // Constructs a <Key, Value(std::make_shared<>(args))> pair in the map if
    // there isn't already one. Returns whether the insertion happened.
    template<class K = Key, class... Args>
    std::pair<bool, Value> tryEmplaceShared(K&& key, Args&&... args)
        requires detail::is_shared_ptr<Value>::value
    {
        FullLock lock{ mutex };
        auto [it, inserted] = map.try_emplace(std::forward<K>(key), nullptr);
        if (inserted) {
            it->second = std::make_shared<typename Value::element_type>(
              std::forward<Args>(args)...);
        }
        return std::make_pair(inserted, it->second);
    }

    // Constructs a <Key, Value(args)> pair in the map if there isn't already
    // one. Calls the given function passing whether a value was emplaced and a
    // reference to value matching the key while holding the map lock. Returns
    // the return value of the passed in function.
    template<class K = Key, std::invocable<bool, Value&> F, class... Args>
    auto tryEmplaceThenMutate(K&& key, F mutator, Args&&... args)
      -> std::invoke_result_t<F, bool, Value&>
    {
        FullLock lock{ mutex };
        auto [it, rawInserted] =
          map.try_emplace(std::forward<K>(key), std::forward<Args>(args)...);
        const bool inserted = rawInserted;
        Value& value = it->second;
        return std::invoke(std::move(mutator), inserted, value);
    }

    // Calls the given function passing a reference to value matching the key
    // while holding the map lock. Returns whether the element was found.
    // Allows heterogeneous lookup.
    template<class K = Key, std::invocable<Value&> F>
    bool mutate(const KeyArg<K>& key, F mutator)
    {
        FullLock lock{ mutex };
        auto it = map.find(key);
        if (it != map.end()) {
            Value& value = it->second;
            std::invoke(std::move(mutator), value);
            return true;
        }
        return false;
    }

    // Calls the given function passing a const reference to value matching the
    // key while holding the map lock. Returns whether the element was found.
    // Allows heterogeneous lookup.
    template<class K = Key, std::invocable<const Value&> F>
    bool inspect(const KeyArg<K>& key, F inspector) const
    {
        SharedLock lock{ mutex };
        auto it = map.find(key);
        if (it != map.end()) {
            const Value& value = it->second;
            std::invoke(std::move(inspector), value);
            return true;
        }
        return false;
    }

    // Returns a copy of the value with the given key, or nullopt if not found.
    // Allows heterogeneous lookup.
    template<class K = Key>
    std::optional<Value> get(const KeyArg<K>& key) const
        requires std::copy_constructible<Value>
    {
        SharedLock lock{ mutex };
        auto it = map.find(key);
        return (it != map.end()) ? std::make_optional(it->second)
                                 : std::nullopt;
    }

    // Checks if an element with the given key exists, allows heterogeneous
    // lookup.
    template<class K = Key>
    bool contains(const KeyArg<K>& key) const
    {
        SharedLock lock{ mutex };
        return map.count(key) != 0;
    }

    // Iterates over the map, calling the given function for each key-value
    // pair.
    template<std::invocable<const Key&, const Value&> F>
    void inspectAll(F inspector) const
    {
        SharedLock lock{ mutex };
        for (const auto& [rawKey, rawValue] : map) {
            // Ensure correct types
            const Key& key = rawKey;
            const Value& value = rawValue;
            std::invoke(inspector, key, value);
        }
    }

    // Iterates over the map holding an exclusive lock, calling the given
    // function for each key-value pair.
    template<std::invocable<const Key&, Value&> F>
    void mutateAll(F mutator)
    {
        FullLock lock{ mutex };
        for (auto& [rawKey, rawValue] : map) {
            // Ensure correct types
            const Key& key = rawKey;
            Value& value = rawValue;
            std::invoke(mutator, key, value);
        }
    }

    // Removes elements matching the given predicate
    template<std::predicate<const Key&, const Value&> F>
    void eraseIf(F predicate)
    {
        FullLock lock{ mutex };
        absl::erase_if(map, [&](const typename UnderlyingMap::value_type& it) {
            const Key& key = it.first;
            const Value& value = it.second;
            return std::invoke(predicate, key, value);
        });
    }

    // Makes a copy of all the Key-Value pairs in the container, sorted by key.
    // Mostly for testing and debugging
    std::vector<std::pair<Key, Value>> sortedKvPairs() const
        requires std::totally_ordered<Key> && std::copy_constructible<Key> &&
                 std::copy_constructible<Value>
    {
        SharedLock lock{ mutex };
        std::vector<std::pair<Key, Value>> pairs;
        pairs.reserve(map.size());
        for (const auto& [rawKey, rawValue] : map) {
            const Key& key = rawKey;
            const Value& value = rawValue;
            pairs.emplace_back(std::piecewise_construct,
                               std::forward_as_tuple(key),
                               std::forward_as_tuple(value));
        }
        std::sort(pairs.begin(), pairs.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });
        return pairs;
    }
};

}
