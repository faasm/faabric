#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/util/batch.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>

#include <fmt/format.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

using namespace faabric::batch_scheduler;

std::pair<HostMap, InFlightReqs>
readOccupationFromFile(const std::string& filePath)
{
    faabric::batch_scheduler::HostMap hostMap;
    InFlightReqs inFlightReqs;

    std::ifstream inFile(filePath);
    if (!inFile.is_open()) {
        std::cout << "Error opening file: "
                  << filePath
                  << std::endl;
        throw std::runtime_error("Error opening file!");
    }

    // Skip header
    std::string line;
    std::getline(inFile, line);

    // Split each comma-separated line
    while (getline(inFile, line)) {
        if (line.empty()) {
            continue;
        }

        // Each line leades with the worker IP, and then an arbitrary number
        // of slots corresponding to each vCPU
        std::istringstream iss(line);
        std::string lineStream;
        std::getline(iss, lineStream, ',');
        std::string hostIp = lineStream;
        hostMap[hostIp] = std::make_shared<HostState>(hostIp, 0, 0);

        while (std::getline(iss, lineStream, ',')) {
            int appId = std::stoi(lineStream);

            // First update the host map
            hostMap.at(hostIp)->slots += 1;
            if (appId != -1) {
                hostMap.at(hostIp)->usedSlots += 1;
            } else {
                continue;
            }

            // Now update the in-flight requests. If an app ID appears in a
            // line for a host, we add a message to that InFlightPair scheduled
            // to this host
            if (!inFlightReqs.contains(appId)) {
                auto ber = faabric::util::batchExecFactory();
                ber->set_user("foo");
                ber->set_function("bar");
                faabric::util::updateBatchExecAppId(ber, appId);
                faabric::util::updateBatchExecGroupId(ber, faabric::util::generateGid());
                auto decision = std::make_shared<SchedulingDecision>(ber->appid(), ber->groupid());
                inFlightReqs[appId] = std::make_pair(ber, decision);
            }

            auto [ber, decision] = inFlightReqs.at(appId);
            auto* newMsg = ber->add_messages();
            newMsg->set_user(ber->user());
            newMsg->set_function(ber->function());
            newMsg->set_appid(ber->appid());
            newMsg->set_groupid(ber->groupid());
            newMsg->set_groupidx(decision->hosts.size());
            decision->addMessage(hostIp, *newMsg);
        }
    }

    return std::make_pair(hostMap, inFlightReqs);
}

/* This binary is meant to be used as an oracle to work out if a given app
 * can be migrated or not. Given an app ID, and a CSV file with the current
 * worker occupation, it returns an exit code of `0` if the app can be migrated
 * or an exit code of `1` if the app cannot be migrated.
 *
 * This method is particularly useful because it uses the exact same logic
 * implemented in the batch scheduler.
 *
 * The CSV file should have the following format
 * $ cat worker_occupation.csv
 * WorkerIp,Slots
 * Ip_1,AppId_1,...,AppId_j,-1
 * ...
 * Ip_m,-1,-1,-1,-1,-1,-1,-1,-1
 *
 * where `-1` indicate empty slots.
 *
 * An example usage of this method is:
 * is_app_migratable AppId /path/to/worker_occupation.csv
 */
int main(int argc, char** argv)
{
    // ------
    // Process command line arguments
    // ------

    if (argc != 3) {
        std::cout << "ERROR: required two positional arguments, got "
                  << argc -1
                  << " instead!"
                  << std::endl;
        throw std::runtime_error("Unexpected number of positional arguments!");
    }

    int32_t appId = std::atoi(argv[1]);
    std::string workerOccupationFilePath = argv[2];

    // ------
    // Prepare variables to get a trustworthy scheduling decision
    // ------

    auto batchScheduler = faabric::batch_scheduler::getBatchScheduler();
    auto [hostMap, inFlightReqs] = readOccupationFromFile(workerOccupationFilePath);

    auto req = inFlightReqs.at(appId).first;
    req->set_type(faabric::BatchExecuteRequest::MIGRATION);

    std::cout << "Preliminary host map check:" << std::endl;
    for (const auto& [ip, host] : hostMap) {
        std::cout << fmt::format("IP: {} - Slots: {}/{}", ip, host->usedSlots, host->slots)
                  << std::endl;
    }

    for (const auto& [appId, inFlightPair] : inFlightReqs) {
        auto decision = inFlightPair.second;
        std::cout << fmt::format("App {} has a decision with {} messages!", appId, decision->hosts.size())
                  << std::endl;
    }

    // ------
    // Work-out whether app can be migrated or not
    // ------

    faabric::util::initLogging();
    auto decision = batchScheduler->makeSchedulingDecision(
      hostMap, inFlightReqs, req);

    if (*decision == DO_NOT_MIGRATE_DECISION) {
        std::cout << "NOT migrating app: " << req->appid() << std::endl;
        return 1;
    }

    std::cout << "Migrating app: " << req->appid() << std::endl;

    return 0;
}
