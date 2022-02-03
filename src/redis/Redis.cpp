#include <faabric/redis/Redis.h>

#include <cstdarg>
#include <faabric/util/bytes.h>
#include <faabric/util/config.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>
#include <faabric/util/random.h>
#include <faabric/util/timing.h>
#include <thread>

namespace faabric::redis {

UniqueRedisReply wrapReply(redisReply* r)
{
    return UniqueRedisReply(r, &freeReplyObject);
}

UniqueRedisReply safeRedisCommand(redisContext* c, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    UniqueRedisReply reply =
      wrapReply((redisReply*)redisvCommand(c, format, args));
    va_end(args);
    return reply;
}

RedisInstance::RedisInstance(RedisRole roleIn)
  : role(roleIn)
{
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();

    if (role == STATE) {
        hostname = conf.redisStateHost;
    } else {
        hostname = conf.redisQueueHost;
    }

    ip = faabric::util::getIPFromHostname(hostname);

    std::string portStr = conf.redisPort;
    port = std::stoi(portStr);

    // Load scripts
    if (delifeqSha.empty() || schedPublishSha.empty()) {
        std::unique_lock<std::mutex> lock(scriptsLock);

        if (delifeqSha.empty() || schedPublishSha.empty()) {
            printf("Loading scripts for Redis instance at %s\n",
                   hostname.c_str());
            redisContext* context = redisConnect(ip.c_str(), port);

            delifeqSha = this->loadScript(context, delifeqCmd);
            schedPublishSha = this->loadScript(context, schedPublishCmd);

            redisFree(context);
        }
    }
}

std::string RedisInstance::loadScript(redisContext* context,
                                      const std::string_view scriptBody)
{
    auto reply = safeRedisCommand(
      context, "SCRIPT LOAD %b", scriptBody.data(), scriptBody.size());

    if (reply == nullptr) {
        throw std::runtime_error("Error loading script from Redis");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        throw std::runtime_error(reply->str);
    }

    std::string scriptSha = reply->str;

    return scriptSha;
}

Redis::Redis(const RedisInstance& instanceIn)
  : instance(instanceIn)
{
    // Connect with IP, not with hostname
    context = redisConnect(instance.ip.c_str(), instance.port);

    if (context == nullptr || context->err) {
        if (context) {
            printf("Error connecting to redis at %s: %s\n",
                   instance.ip.c_str(),
                   context->errstr);
        } else {
            printf("Error allocating redis context\n");
        }

        throw std::runtime_error("Failed to connect to redis");
    }
}

Redis::~Redis()
{
    redisFree(context);
}

/**
 *  ------ Utils ------
 */

Redis& Redis::getState()
{
    // Hiredis requires one instance per thread
    static RedisInstance stateInstance(STATE);
    static thread_local redis::Redis redisState(stateInstance);
    return redisState;
}

Redis& Redis::getQueue()
{
    // Hiredis requires one instance per thread
    static RedisInstance queueInstance(QUEUE);
    static thread_local redis::Redis redisQueue(queueInstance);
    return redisQueue;
}

long getLongFromReply(const redisReply& reply)
{
    long res = 0;

    if (reply.str != nullptr) {
        res = std::stol(reply.str);
    }

    return res;
}

std::vector<uint8_t> getBytesFromReply(const redisReply& reply)
{
    // We have to be careful here to handle the bytes properly
    const char* resultArray = reply.str;
    const size_t resultLen = reply.len;

    std::vector<uint8_t> resultData(resultArray, resultArray + resultLen);

    return resultData;
}

void getBytesFromReply(const std::string& key,
                       const redisReply& reply,
                       uint8_t* buffer,
                       size_t bufferLen)
{
    // We have to be careful here to handle the bytes properly
    const char* resultArray = reply.str;
    const size_t resultLen = reply.len;

    if (resultLen > (int)bufferLen) {
        SPDLOG_ERROR("Value ({}) too big for buffer ({}) - key {}",
                     resultLen,
                     bufferLen,
                     key);
        throw std::runtime_error("Reading value too big for buffer");
    }

    std::copy_n(resultArray, resultLen, buffer);
}

/**
 *  ------ Lua scripts ------
 */

long extractScriptResult(const redisReply& reply)
{
    if (reply.type == REDIS_REPLY_ERROR) {
        throw(std::runtime_error(reply.str));
    }

    long result = reply.integer;

    return result;
}

/**
 *  ------ Standard Redis commands ------
 */

void Redis::ping()
{

    SPDLOG_DEBUG("Pinging redis at {}", instance.hostname);
    auto reply = safeRedisCommand(context, "PING");

    std::string response(reply->str);

    if (response != "PONG") {
        SPDLOG_DEBUG("Failed pinging redis at {}", instance.hostname);
        throw std::runtime_error("Failed to ping redis host");
    }

    SPDLOG_DEBUG("Successfully pinged redis");
}

size_t Redis::strlen(const std::string& key)
{
    auto reply = safeRedisCommand(context, "STRLEN %s", key.c_str());

    size_t result = reply->integer;
    return result;
}

void Redis::get(const std::string& key, uint8_t* buffer, size_t size)
{
    auto reply = safeRedisCommand(context, "GET %s", key.c_str());

    getBytesFromReply(key, *reply, buffer, size);
}

std::vector<uint8_t> Redis::get(const std::string& key)
{
    auto reply = safeRedisCommand(context, "GET %s", key.c_str());

    std::vector<uint8_t> replyBytes = getBytesFromReply(*reply);

    return replyBytes;
}

long Redis::getCounter(const std::string& key)
{
    auto reply = safeRedisCommand(context, "GET %s", key.c_str());

    if (reply == nullptr || reply->type == REDIS_REPLY_NIL || reply->len == 0) {
        return 0;
    }

    return std::stol(reply->str);
}

long Redis::incr(const std::string& key)
{
    auto reply = safeRedisCommand(context, "INCR %s", key.c_str());

    long result = reply->integer;
    return result;
}

long Redis::decr(const std::string& key)
{
    auto reply = safeRedisCommand(context, "DECR %s", key.c_str());
    long result = reply->integer;

    return result;
}

long Redis::incrByLong(const std::string& key, long val)
{
    // Format is NOT printf compatible contrary to what the docs say, hence %i
    // instead of %l.
    auto reply = safeRedisCommand(context, "INCRBY %s %i", key.c_str(), val);

    long result = reply->integer;

    return result;
}

long Redis::decrByLong(const std::string& key, long val)
{
    // Format is NOT printf compatible contrary to what the docs say, hence %i
    // instead of %l.
    auto reply = safeRedisCommand(context, "DECRBY %s %i", key.c_str(), val);

    long result = reply->integer;

    return result;
}

void Redis::set(const std::string& key, const std::vector<uint8_t>& value)
{
    this->set(key, value.data(), value.size());
}

void Redis::set(const std::string& key, const uint8_t* value, size_t size)
{
    auto reply =
      safeRedisCommand(context, "SET %s %b", key.c_str(), value, size);

    if (reply->type == REDIS_REPLY_ERROR) {
        SPDLOG_ERROR("Failed to SET {} - {}", key.c_str(), reply->str);
    }
}

void Redis::del(const std::string& key)
{
    safeRedisCommand(context, "DEL %s", key.c_str());
}

void Redis::setRange(const std::string& key,
                     long offset,
                     const uint8_t* value,
                     size_t size)
{
    auto reply = safeRedisCommand(
      context, "SETRANGE %s %li %b", key.c_str(), offset, value, size);

    if (reply->type != REDIS_REPLY_INTEGER) {
        SPDLOG_ERROR("Failed SETRANGE {}", key);
        throw std::runtime_error("Failed SETRANGE " + key);
    }
}

void Redis::setRangePipeline(const std::string& key,
                             long offset,
                             const uint8_t* value,
                             size_t size)
{
    redisAppendCommand(
      context, "SETRANGE %s %li %b", key.c_str(), offset, value, size);
}

void Redis::flushPipeline(long pipelineLength)
{
    void* reply;
    for (long p = 0; p < pipelineLength; p++) {
        redisGetReply(context, &reply);
        auto _replyGuard = wrapReply((redisReply*)reply);

        if (reply == nullptr ||
            ((redisReply*)reply)->type == REDIS_REPLY_ERROR) {
            SPDLOG_ERROR("Failed pipeline call {}", p);
            throw std::runtime_error("Failed pipeline call " +
                                     std::to_string(p));
        }
    }
}

void Redis::sadd(const std::string& key, const std::string& value)
{
    auto reply =
      safeRedisCommand(context, "SADD %s %s", key.c_str(), value.c_str());
    if (reply->type == REDIS_REPLY_ERROR) {
        SPDLOG_ERROR("Failed to add {} to set {}", value, key);
        throw std::runtime_error("Failed to add element to set");
    }
}

void Redis::srem(const std::string& key, const std::string& value)
{

    safeRedisCommand(context, "SREM %s %s", key.c_str(), value.c_str());
}

long Redis::scard(const std::string& key)
{
    auto reply = safeRedisCommand(context, "SCARD %s", key.c_str());

    long res = reply->integer;

    return res;
}

bool Redis::sismember(const std::string& key, const std::string& value)
{
    auto reply =
      safeRedisCommand(context, "SISMEMBER %s %s", key.c_str(), value.c_str());

    bool res = reply->integer == 1;

    return res;
}

std::string Redis::srandmember(const std::string& key)
{
    auto reply = safeRedisCommand(context, "SRANDMEMBER %s", key.c_str());

    std::string res;
    if (reply->len > 0) {
        res = reply->str;
    }

    return res;
}

std::set<std::string> extractStringSetFromReply(const redisReply& reply)
{
    std::set<std::string> retValue;
    for (size_t i = 0; i < reply.elements; i++) {
        retValue.insert(reply.element[i]->str);
    }

    return retValue;
}

std::set<std::string> Redis::smembers(const std::string& key)
{
    auto reply = safeRedisCommand(context, "SMEMBERS %s", key.c_str());
    std::set<std::string> result = extractStringSetFromReply(*reply);

    return result;
}

std::set<std::string> Redis::sinter(const std::string& keyA,
                                    const std::string& keyB)
{
    auto reply =
      safeRedisCommand(context, "SINTER %s %s", keyA.c_str(), keyB.c_str());
    std::set<std::string> result = extractStringSetFromReply(*reply);

    return result;
}

std::set<std::string> Redis::sdiff(const std::string& keyA,
                                   const std::string& keyB)
{
    auto reply =
      safeRedisCommand(context, "SDIFF %s %s", keyA.c_str(), keyB.c_str());
    std::set<std::string> result = extractStringSetFromReply(*reply);

    return result;
}

int Redis::lpushLong(const std::string& key, long value)
{
    auto reply = safeRedisCommand(context, "LPUSH %s %i", key.c_str(), value);
    long long int result = reply->integer;

    return result;
}

int Redis::rpushLong(const std::string& key, long value)
{
    auto reply = safeRedisCommand(context, "RPUSH %s %i", key.c_str(), value);
    long long int result = reply->integer;
    return result;
}

void Redis::flushAll()
{
    safeRedisCommand(context, "FLUSHALL");
}

long Redis::listLength(const std::string& queueName)
{
    auto reply = safeRedisCommand(context, "LLEN %s", queueName.c_str());

    if (reply == nullptr || reply->type == REDIS_REPLY_NIL) {
        return 0;
    }

    long result = reply->integer;

    return result;
}

long Redis::getTtl(const std::string& key)
{
    auto reply = safeRedisCommand(context, "TTL %s", key.c_str());

    long ttl = reply->integer;

    return ttl;
}

void Redis::expire(const std::string& key, long expiry)
{
    safeRedisCommand(context, "EXPIRE %s %d", key.c_str(), expiry);
}

void Redis::refresh()
{
    redisReconnect(context);
}

/**
 * Note that start/end are both inclusive
 */
void Redis::getRange(const std::string& key,
                     uint8_t* buffer,
                     size_t bufferLen,
                     long start,
                     long end)
{
    size_t rangeLen = (size_t)end - start;
    if (rangeLen > bufferLen) {
        throw std::runtime_error(
          "Range " + std::to_string(start) + "-" + std::to_string(end) +
          " too long for buffer length " + std::to_string(bufferLen));
    }

    auto reply =
      safeRedisCommand(context, "GETRANGE %s %li %li", key.c_str(), start, end);

    // Importantly getrange is inclusive so we need to be checking the buffer
    // length
    getBytesFromReply(key, *reply, buffer, bufferLen);
}

/**
 *  ------ Locking ------
 */

uint32_t Redis::acquireLock(const std::string& key, int expirySeconds)
{
    // Implementation of single host redlock algorithm
    // https://redis.io/topics/distlock
    uint32_t lockId = faabric::util::generateGid();

    std::string lockKey = key + "_lock";
    bool result = this->setnxex(lockKey, lockId, expirySeconds);

    if (result) {
        return lockId;
    } else {
        return 0;
    }
}

void Redis::releaseLock(const std::string& key, uint32_t lockId)
{
    std::string lockKey = key + "_lock";
    this->delIfEq(lockKey, lockId);
}

void Redis::delIfEq(const std::string& key, uint32_t value)
{
    // Invoke the script
    auto reply = safeRedisCommand(context,
                                  "EVALSHA %s 1 %s %i",
                                  instance.delifeqSha.c_str(),
                                  key.c_str(),
                                  value);

    extractScriptResult(*reply);
}

bool Redis::setnxex(const std::string& key, long value, int expirySeconds)
{
    // See docs on set for info on options: https://redis.io/commands/set
    // We use NX to say "set if not exists" and ex to specify the expiry of this
    // key/value This is useful in implementing locks. We only use longs as
    // values to keep things simple
    auto reply = safeRedisCommand(
      context, "SET %s %i EX %i NX", key.c_str(), value, expirySeconds);

    bool success = false;
    if (reply->type == REDIS_REPLY_ERROR) {
        SPDLOG_ERROR("Failed to SET {} - {}", key.c_str(), reply->str);
    } else if (reply->type == REDIS_REPLY_STATUS) {
        success = true;
    }

    return success;
}

long Redis::getLong(const std::string& key)
{
    auto reply = safeRedisCommand(context, "GET %s", key.c_str());

    long res = getLongFromReply(*reply);

    return res;
}

void Redis::setLong(const std::string& key, long value)
{
    // Format is NOT printf compatible contrary to what the docs say, hence %i
    // instead of %l.
    safeRedisCommand(context, "SET %s %i", key.c_str(), value);
}

/**
 *  ------ Queueing ------
 */

void Redis::enqueue(const std::string& queueName, const std::string& value)
{
    safeRedisCommand(context, "RPUSH %s %s", queueName.c_str(), value.c_str());
}

void Redis::enqueueBytes(const std::string& queueName,
                         const std::vector<uint8_t>& value)
{
    enqueueBytes(queueName, value.data(), value.size());
}

void Redis::enqueueBytes(const std::string& queueName,
                         const uint8_t* buffer,
                         size_t bufferLen)
{
    // NOTE: Here we must be careful with the input and specify bytes rather
    // than a string otherwise an encoded false boolean can be treated as a
    // string terminator
    auto reply = safeRedisCommand(
      context, "RPUSH %s %b", queueName.c_str(), buffer, bufferLen);

    if (reply->type != REDIS_REPLY_INTEGER) {
        throw std::runtime_error("Failed to enqueue bytes. Reply type = " +
                                 std::to_string(reply->type));
    } else if (reply->integer <= 0) {
        throw std::runtime_error("Failed to enqueue bytes. Length = " +
                                 std::to_string(reply->integer));
    }
}

UniqueRedisReply Redis::dequeueBase(const std::string& queueName, int timeoutMs)
{
    // NOTE - we contradict the default redis behaviour here by doing a
    // non-blocking pop when timeout is zero (rather than infinite as in Redis)
    bool isBlocking = timeoutMs > 0;

    UniqueRedisReply reply{ nullptr, &freeReplyObject };
    if (isBlocking) {
        // Timeouts need to be converted into seconds
        // Floor to one second
        int timeoutSecs = std::max(timeoutMs / 1000, 1);

        reply = safeRedisCommand(
          context, "BLPOP %s %d", queueName.c_str(), timeoutSecs);
    } else {
        // LPOP is non-blocking
        reply = safeRedisCommand(context, "LPOP %s", queueName.c_str());
    }

    // Check if we got anything
    if (reply == nullptr || reply->type == REDIS_REPLY_NIL) {
        std::string msg =
          fmt::format("No response from Redis dequeue in {}ms for queue {}",
                      timeoutMs,
                      queueName);
        throw RedisNoResponseException(msg);
    }

    // Should get an array when doing a blpop, check it.
    if (isBlocking) {
        if (reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Failed dequeue: " +
                                     std::string(reply->str));
        } else if (reply->type != REDIS_REPLY_ARRAY) {
            throw std::runtime_error(
              "Expected array response from BLPOP but got " +
              std::to_string(reply->type));
        }

        size_t nResults = reply->elements;

        if (nResults > 2) {
            throw std::runtime_error(
              "Returned more than one pair of dequeued values");
        }
    }

    return reply;
}

std::string Redis::dequeue(const std::string& queueName, int timeoutMs)
{
    bool isBlocking = timeoutMs > 0;
    auto reply = this->dequeueBase(queueName, timeoutMs);

    std::string result;
    if (isBlocking) {
        redisReply* r = reply->element[1];
        result = r->str;
    } else {
        result = reply->str;
    }

    return result;
}

void Redis::dequeueMultiple(const std::string& queueName,
                            uint8_t* buff,
                            long buffLen,
                            long nElems)
{
    // NOTE - much like other range stuff with redis, this is *INCLUSIVE*
    auto reply = safeRedisCommand(
      context, "LRANGE %s 0 %i", queueName.c_str(), nElems - 1);

    long offset = 0;
    for (size_t i = 0; i < reply->elements; i++) {
        redisReply* r = reply->element[i];
        std::copy(r->str, r->str + r->len, buff + offset);
        offset += r->len;
    }

    if (offset > buffLen) {
        throw std::runtime_error("Copied over end of buffer (copied " +
                                 std::to_string(offset) + " buffer " +
                                 std::to_string(buffLen) + ")");
    }
}

std::vector<uint8_t> Redis::dequeueBytes(const std::string& queueName,
                                         int timeoutMs)
{
    bool isBlocking = timeoutMs > 0;
    auto reply = this->dequeueBase(queueName, timeoutMs);

    std::vector<uint8_t> replyBytes;
    if (isBlocking) {
        // BLPOP will return the queue name and the value returned (elements
        // 0 and 1)
        replyBytes = getBytesFromReply(*reply->element[1]);
    } else {
        replyBytes = getBytesFromReply(*reply);
    }

    return replyBytes;
}

size_t Redis::dequeueBytes(const std::string& queueName,
                           uint8_t* buffer,
                           size_t bufferLen,
                           int timeoutMs)
{
    bool isBlocking = timeoutMs > 0;
    auto replyOwned = this->dequeueBase(queueName, timeoutMs);
    auto reply = replyOwned.get();

    if (isBlocking) {
        reply = reply->element[1];
    }

    auto resultBytes = (uint8_t*)reply->str;
    size_t resultLen = reply->len;

    if (resultLen > bufferLen) {
        throw std::runtime_error(
          "Buffer not long enough for dequeue result (buffer=" +
          std::to_string(bufferLen) + " len=" + std::to_string(resultLen) +
          ")");
    }

    ::memcpy(buffer, resultBytes, resultLen);
    return resultLen;
}

void Redis::publishSchedulerResult(const std::string& key,
                                   const std::string& status_key,
                                   const std::vector<uint8_t>& result)
{
    auto reply = safeRedisCommand(context,
                                  "EVALSHA %s 2 %s %s %b %d %d",
                                  instance.schedPublishSha.c_str(),
                                  // keys
                                  key.c_str(),
                                  status_key.c_str(),
                                  // argv
                                  result.data(),
                                  result.size(),
                                  RESULT_KEY_EXPIRY,
                                  STATUS_KEY_EXPIRY);
    extractScriptResult(*reply);
}
}
