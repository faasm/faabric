#include <catch2/catch.hpp>

#include "faabric_utils.h"

namespace tests {
void checkMessageEquality(const faabric::Message& msgA,
                          const faabric::Message& msgB)
{
    REQUIRE(msgA.id() == msgB.id());
    REQUIRE(msgA.type() == msgB.type());

    REQUIRE(msgA.user() == msgB.user());
    REQUIRE(msgA.function() == msgB.function());
    REQUIRE(msgA.executedhost() == msgB.executedhost());
    REQUIRE(msgA.finishtimestamp() == msgB.finishtimestamp());

    REQUIRE(msgA.timestamp() == msgB.timestamp());
    REQUIRE(msgA.snapshotkey() == msgB.snapshotkey());
    REQUIRE(msgA.funcptr() == msgB.funcptr());

    REQUIRE(msgA.pythonuser() == msgB.pythonuser());
    REQUIRE(msgA.pythonfunction() == msgB.pythonfunction());
    REQUIRE(msgA.pythonentry() == msgB.pythonentry());
    REQUIRE(msgA.isasync() == msgB.isasync());
    REQUIRE(msgA.ispython() == msgB.ispython());
    REQUIRE(msgA.isstatusrequest() == msgB.isstatusrequest());
    REQUIRE(msgA.isexecgraphrequest() == msgB.isexecgraphrequest());

    REQUIRE(msgA.returnvalue() == msgB.returnvalue());

    REQUIRE(msgA.inputdata() == msgB.inputdata());
    REQUIRE(msgA.outputdata() == msgB.outputdata());

    REQUIRE(msgA.resultkey() == msgB.resultkey());
    REQUIRE(msgA.statuskey() == msgB.statuskey());

    REQUIRE(msgA.ismpi() == msgB.ismpi());
    REQUIRE(msgA.mpiworldid() == msgB.mpiworldid());
    REQUIRE(msgA.mpirank() == msgB.mpirank());
    REQUIRE(msgA.mpiworldsize() == msgB.mpiworldsize());

    REQUIRE(msgA.cmdline() == msgB.cmdline());

    REQUIRE(msgA.recordexecgraph() == msgB.recordexecgraph());
    checkMessageMapEquality(msgA.execgraphdetails(), msgB.execgraphdetails());
    checkMessageMapEquality(msgA.intexecgraphdetails(),
                            msgB.intexecgraphdetails());

    REQUIRE(msgA.migrationcheckperiod() == msgB.migrationcheckperiod());
}
}
