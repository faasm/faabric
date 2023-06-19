#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/scheduler/ExecutorContext.h>
#include <faabric/util/func.h>

using namespace faabric::scheduler;

namespace tests {

TEST_CASE_METHOD(ExecutorContextFixture, "Test executor context", "[scheduler]")
{
    REQUIRE(!ExecutorContext::isSet());

    // Getting with no context should fail
    REQUIRE_THROWS(ExecutorContext::get());

    faabric::Message msg = faabric::util::messageFactory("foo", "bar");

    std::shared_ptr<DummyExecutorFactory> fac =
      std::make_shared<DummyExecutorFactory>();
    auto exec = fac->createExecutor(msg);

    auto req = faabric::util::batchExecFactory("foo", "bar", 5);

    SECTION("Set both executor and request")
    {
        ExecutorContext::set(exec.get(), req, 3);

        std::shared_ptr<ExecutorContext> ctx = ExecutorContext::get();
        REQUIRE(ctx->getExecutor() == exec.get());
        REQUIRE(ctx->getBatchRequest() == req);
        REQUIRE(ctx->getMsgIdx() == 3);
        REQUIRE(ctx->getMsg().id() == req->mutable_messages()->at(3).id());
    }

    SECTION("Just set executor")
    {
        ExecutorContext::set(exec.get(), nullptr, 0);

        std::shared_ptr<ExecutorContext> ctx = ExecutorContext::get();
        REQUIRE(ctx->getExecutor() == exec.get());
        REQUIRE(ctx->getBatchRequest() == nullptr);
        REQUIRE(ctx->getMsgIdx() == 0);

        REQUIRE_THROWS(ctx->getMsg());
    }

    SECTION("Just set request")
    {
        ExecutorContext::set(nullptr, req, 3);

        std::shared_ptr<ExecutorContext> ctx = ExecutorContext::get();
        REQUIRE(ctx->getExecutor() == nullptr);
        REQUIRE(ctx->getBatchRequest() == req);
        REQUIRE(ctx->getMsgIdx() == 3);
        REQUIRE(ctx->getMsg().id() == req->mutable_messages()->at(3).id());
    }

    ExecutorContext::unset();
    REQUIRE_THROWS(ExecutorContext::get());

    exec->shutdown();
}
}
