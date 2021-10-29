#include <faabric/util/json.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cppcodec/base64_rfc4648.hpp>

#include <sstream>

using namespace rapidjson;

namespace faabric::util {
std::string messageToJson(const faabric::Message& msg)
{
    Document d;
    d.SetObject();
    Document::AllocatorType& a = d.GetAllocator();

    // Need to be explicit with strings here to make a copy _and_ make sure we
    // specify the length to include any null-terminators from bytes
    d.AddMember("id", msg.id(), a);
    d.AddMember("type", msg.type(), a);
    d.AddMember(
      "user", Value(msg.user().c_str(), msg.user().size(), a).Move(), a);
    d.AddMember("function",
                Value(msg.function().c_str(), msg.function().size(), a).Move(),
                a);

    if (!msg.executedhost().empty()) {
        d.AddMember(
          "exec_host",
          Value(msg.executedhost().c_str(), msg.executedhost().size(), a)
            .Move(),
          a);
    }

    if (!msg.masterhost().empty()) {
        d.AddMember(
          "master_host",
          Value(msg.masterhost().c_str(), msg.masterhost().size(), a).Move(),
          a);
    }

    if (msg.finishtimestamp() > 0) {
        d.AddMember("finished", msg.finishtimestamp(), a);
    }

    if (msg.timestamp() > 0) {
        d.AddMember("timestamp", msg.timestamp(), a);
    }

    if (!msg.snapshotkey().empty()) {
        d.AddMember(
          "snapshot_key",
          Value(msg.snapshotkey().c_str(), msg.snapshotkey().size(), a).Move(),
          a);
    }

    if (msg.funcptr() > 0) {
        d.AddMember("func_ptr", msg.funcptr(), a);
    }

    if (!msg.pythonuser().empty()) {
        d.AddMember(
          "py_user",
          Value(msg.pythonuser().c_str(), msg.pythonuser().size(), a).Move(),
          a);
    }

    if (!msg.pythonfunction().empty()) {
        d.AddMember(
          "py_func",
          Value(msg.pythonfunction().c_str(), msg.pythonfunction().size(), a)
            .Move(),
          a);
    }

    if (!msg.pythonentry().empty()) {
        d.AddMember(
          "py_entry",
          Value(msg.pythonentry().c_str(), msg.pythonentry().size(), a).Move(),
          a);
    }

    if (!msg.inputdata().empty()) {
        d.AddMember(
          "input_data",
          Value(msg.inputdata().c_str(), msg.inputdata().size(), a).Move(),
          a);
    }

    if (!msg.outputdata().empty()) {
        d.AddMember(
          "output_data",
          Value(msg.outputdata().c_str(), msg.outputdata().size(), a).Move(),
          a);
    }

    if (msg.isasync()) {
        d.AddMember("async", msg.isasync(), a);
    }

    if (msg.ispython()) {
        d.AddMember("python", msg.ispython(), a);
    }

    if (msg.isstatusrequest()) {
        d.AddMember("status", msg.isstatusrequest(), a);
    }

    if (msg.isexecgraphrequest()) {
        d.AddMember("exec_graph", msg.isexecgraphrequest(), a);
    }

    if (!msg.resultkey().empty()) {
        d.AddMember(
          "result_key",
          Value(msg.resultkey().c_str(), msg.resultkey().size()).Move(),
          a);
    }

    if (!msg.statuskey().empty()) {
        d.AddMember(
          "status_key",
          Value(msg.statuskey().c_str(), msg.statuskey().size()).Move(),
          a);
    }

    if (msg.ismpi()) {
        d.AddMember("mpi", msg.ismpi(), a);
    }

    if (msg.mpiworldid() > 0) {
        d.AddMember("mpi_world_id", msg.mpiworldid(), a);
    }

    if (msg.mpirank() > 0) {
        d.AddMember("mpi_rank", msg.mpirank(), a);
    }

    if (msg.mpiworldsize() > 0) {
        d.AddMember("mpi_world_size", msg.mpiworldsize(), a);
    }

    if (!msg.cmdline().empty()) {
        d.AddMember("cmdline",
                    Value(msg.cmdline().c_str(), msg.cmdline().size()).Move(),
                    a);
    }

    if (msg.issgx()) {
        d.AddMember("sgx", msg.issgx(), a);
    }

    if (!msg.sgxsid().empty()) {
        d.AddMember(
          "sgxsid", Value(msg.sgxsid().c_str(), msg.sgxsid().size()).Move(), a);
    }

    if (!msg.sgxnonce().empty()) {
        d.AddMember("sgxnonce",
                    Value(msg.sgxnonce().c_str(), msg.sgxnonce().size()).Move(),
                    a);
    }

    if (!msg.sgxtag().empty()) {
        d.AddMember(
          "sgxtag", Value(msg.sgxtag().c_str(), msg.sgxtag().size()).Move(), a);
    }

    if (!msg.sgxpolicy().empty()) {
        d.AddMember(
          "sgxpolicy",
          Value(msg.sgxpolicy().c_str(), msg.sgxpolicy().size()).Move(),
          a);
    }

    if (!msg.sgxresult().empty()) {
        d.AddMember(
          "sgxresult",
          Value(msg.sgxresult().c_str(), msg.sgxresult().size()).Move(),
          a);
    }

    if (msg.recordexecgraph()) {
        d.AddMember("record_exec_graph", msg.recordexecgraph(), a);

        if (msg.execgraphdetails_size() > 0) {
            std::stringstream ss;
            const auto& map = msg.execgraphdetails();
            auto it = map.begin();
            while (it != map.end()) {
                ss << fmt::format("{}:{}", it->first, it->second);
                ++it;
                if (it != map.end()) {
                    ss << ",";
                }
            }

            std::string out = ss.str();
            d.AddMember(
              "exec_graph_detail", Value(out.c_str(), out.size()).Move(), a);
        }

        if (msg.intexecgraphdetails_size() > 0) {
            std::stringstream ss;
            const auto& map = msg.intexecgraphdetails();
            auto it = map.begin();
            while (it != map.end()) {
                ss << fmt::format("{}:{}", it->first, it->second);
                ++it;
                if (it != map.end()) {
                    ss << ",";
                }
            }

            std::string out = ss.str();
            d.AddMember("int_exec_graph_detail",
                        Value(out.c_str(), out.size()).Move(),
                        a);
        }
    }

    StringBuffer sb;
    Writer<StringBuffer> writer(sb);
    d.Accept(writer);

    return sb.GetString();
}

std::string getJsonOutput(const faabric::Message& msg)
{
    Document d;
    d.SetObject();
    Document::AllocatorType& a = d.GetAllocator();
    std::string result_ = cppcodec::base64_rfc4648::encode(msg.sgxresult());
    d.AddMember("result", Value(result_.c_str(), result_.size(), a).Move(), a);
    d.AddMember(
      "output_data",
      Value(msg.outputdata().c_str(), msg.outputdata().size(), a).Move(),
      a);
    StringBuffer sb;
    Writer<StringBuffer> writer(sb);
    d.Accept(writer);
    return sb.GetString();
}

bool getBoolFromJson(Document& doc, const std::string& key, bool dflt)
{
    Value::MemberIterator it = doc.FindMember(key.c_str());
    if (it == doc.MemberEnd()) {
        return dflt;
    }

    return it->value.GetBool();
}

std::vector<uint32_t> getUintArrayFromJson(Document& doc,
                                           const std::string& key)
{
    Value::MemberIterator it = doc.FindMember(key.c_str());
    std::vector<uint32_t> result;
    if (it == doc.MemberEnd()) {
        return result;
    }

    for (const auto& i : it->value.GetArray()) {
        result.emplace_back(i.GetUint());
    }

    return result;
}

int getIntFromJson(Document& doc, const std::string& key, int dflt)
{
    Value::MemberIterator it = doc.FindMember(key.c_str());
    if (it == doc.MemberEnd()) {
        return dflt;
    }

    return it->value.GetInt();
}

int64_t getInt64FromJson(Document& doc, const std::string& key, int dflt)
{
    Value::MemberIterator it = doc.FindMember(key.c_str());
    if (it == doc.MemberEnd()) {
        return dflt;
    }

    return it->value.GetInt64();
}

std::string getStringFromJson(Document& doc,
                              const std::string& key,
                              const std::string& dflt)
{
    Value::MemberIterator it = doc.FindMember(key.c_str());
    if (it == doc.MemberEnd()) {
        return dflt;
    }

    const char* valuePtr = it->value.GetString();
    return std::string(valuePtr, valuePtr + it->value.GetStringLength());
}

std::map<std::string, std::string> getStringStringMapFromJson(
  Document& doc,
  const std::string& key)
{
    std::map<std::string, std::string> map;

    Value::MemberIterator it = doc.FindMember(key.c_str());
    if (it == doc.MemberEnd()) {
        return map;
    }

    const char* valuePtr = it->value.GetString();
    std::stringstream ss(
      std::string(valuePtr, valuePtr + it->value.GetStringLength()));
    std::string keyVal;
    while (std::getline(ss, keyVal, ',')) {
        auto pos = keyVal.find(":");
        std::string key = keyVal.substr(0, pos);
        map[key] = keyVal.erase(0, pos + sizeof(char));
    }

    return map;
}

std::map<std::string, int> getStringIntMapFromJson(Document& doc,
                                                   const std::string& key)
{
    std::map<std::string, int> map;

    Value::MemberIterator it = doc.FindMember(key.c_str());
    if (it == doc.MemberEnd()) {
        return map;
    }

    const char* valuePtr = it->value.GetString();
    std::stringstream ss(
      std::string(valuePtr, valuePtr + it->value.GetStringLength()));
    std::string keyVal;
    while (std::getline(ss, keyVal, ',')) {
        auto pos = keyVal.find(":");
        std::string key = keyVal.substr(0, pos);
        int val = std::stoi(keyVal.erase(0, pos + sizeof(char)));
        map[key] = val;
    }

    return map;
}

faabric::Message jsonToMessage(const std::string& jsonIn)
{
    PROF_START(jsonDecode)

    MemoryStream ms(jsonIn.c_str(), jsonIn.size());
    Document d;
    d.ParseStream(ms);

    faabric::Message msg;

    // Set the message type
    int msgType = getIntFromJson(d, "type", 0);
    if (!faabric::Message::MessageType_IsValid(msgType)) {
        SPDLOG_ERROR("Bad message type: {}", msgType);
        throw std::runtime_error("Invalid message type");
    }
    msg.set_type(static_cast<faabric::Message::MessageType>(msgType));

    msg.set_timestamp(getInt64FromJson(d, "timestamp", 0));
    msg.set_id(getIntFromJson(d, "id", 0));
    msg.set_user(getStringFromJson(d, "user", ""));
    msg.set_function(getStringFromJson(d, "function", ""));
    msg.set_executedhost(getStringFromJson(d, "exec_host", ""));
    msg.set_masterhost(getStringFromJson(d, "master_host", ""));
    msg.set_finishtimestamp(getInt64FromJson(d, "finished", 0));

    msg.set_snapshotkey(getStringFromJson(d, "snapshot_key", ""));
    msg.set_funcptr(getIntFromJson(d, "func_ptr", 0));

    msg.set_pythonuser(getStringFromJson(d, "py_user", ""));
    msg.set_pythonfunction(getStringFromJson(d, "py_func", ""));
    msg.set_pythonentry(getStringFromJson(d, "py_entry", ""));

    msg.set_inputdata(getStringFromJson(d, "input_data", ""));
    msg.set_outputdata(getStringFromJson(d, "output_data", ""));

    msg.set_isasync(getBoolFromJson(d, "async", false));
    msg.set_ispython(getBoolFromJson(d, "python", false));
    msg.set_isstatusrequest(getBoolFromJson(d, "status", false));
    msg.set_isexecgraphrequest(getBoolFromJson(d, "exec_graph", false));

    msg.set_resultkey(getStringFromJson(d, "result_key", ""));
    msg.set_statuskey(getStringFromJson(d, "status_key", ""));

    msg.set_ismpi(getBoolFromJson(d, "mpi", false));
    msg.set_mpiworldid(getIntFromJson(d, "mpi_world_id", 0));
    msg.set_mpirank(getIntFromJson(d, "mpi_rank", 0));
    msg.set_mpiworldsize(getIntFromJson(d, "mpi_world_size", 0));

    msg.set_cmdline(getStringFromJson(d, "cmdline", ""));

    msg.set_issgx(getBoolFromJson(d, "sgx", false));
    msg.set_sgxsid(getStringFromJson(d, "sgxsid", ""));
    msg.set_sgxnonce(getStringFromJson(d, "sgxnonce", ""));
    msg.set_sgxtag(getStringFromJson(d, "sgxtag", ""));
    msg.set_sgxpolicy(getStringFromJson(d, "sgxpolicy", ""));
    msg.set_sgxresult(getStringFromJson(d, "sgxresult", ""));

    msg.set_recordexecgraph(getBoolFromJson(d, "record_exec_graph", false));

    // By default, clear the map
    msg.clear_execgraphdetails();
    // Fill keypairs if found
    auto& msgStrMap = *msg.mutable_execgraphdetails();
    std::map<std::string, std::string> strMap =
      getStringStringMapFromJson(d, "exec_graph_detail");
    for (auto& it : strMap) {
        msgStrMap[it.first] = it.second;
    }

    // By default, clear the map
    msg.clear_intexecgraphdetails();
    // Fill keypairs if found
    auto& msgIntMap = *msg.mutable_intexecgraphdetails();
    std::map<std::string, int> intMap =
      getStringIntMapFromJson(d, "int_exec_graph_detail");
    for (auto& it : intMap) {
        msgIntMap[it.first] = it.second;
    }

    PROF_END(jsonDecode)

    return msg;
}

std::string getValueFromJsonString(const std::string& key,
                                   const std::string& jsonIn)
{
    MemoryStream ms(jsonIn.c_str(), jsonIn.size());
    Document d;
    d.ParseStream(ms);

    std::string result = getStringFromJson(d, key, "");
    return result;
}
}
