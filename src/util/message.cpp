#include <faabric/util/message.h>

namespace faabric::util {
void copyMessage(const faabric::Message* src, faabric::Message* dst)
{
    dst->set_id(src->id());
    dst->set_appid(src->appid());
    dst->set_appidx(src->appidx());
    dst->set_masterhost(src->masterhost());

    dst->set_type(src->type());
    dst->set_user(src->user());
    dst->set_function(src->function());

    dst->set_inputdata(src->inputdata());
    dst->set_outputdata(src->outputdata());

    dst->set_funcptr(src->funcptr());
    dst->set_returnvalue(src->returnvalue());

    dst->set_snapshotkey(src->snapshotkey());

    dst->set_timestamp(src->timestamp());
    dst->set_resultkey(src->resultkey());
    dst->set_executeslocally(src->executeslocally());
    dst->set_statuskey(src->statuskey());

    dst->set_executedhost(src->executedhost());
    dst->set_finishtimestamp(src->finishtimestamp());

    dst->set_isasync(src->isasync());
    dst->set_ispython(src->ispython());
    dst->set_isstatusrequest(src->isstatusrequest());
    dst->set_isexecgraphrequest(src->isexecgraphrequest());

    dst->set_pythonuser(src->pythonuser());
    dst->set_pythonfunction(src->pythonfunction());
    dst->set_pythonentry(src->pythonentry());

    dst->set_groupid(src->groupid());
    dst->set_groupidx(src->groupidx());
    dst->set_groupsize(src->groupsize());

    dst->set_ismpi(src->ismpi());
    dst->set_mpiworldid(src->mpiworldid());
    dst->set_mpirank(src->mpirank());
    dst->set_mpiworldsize(src->mpiworldsize());

    dst->set_cmdline(src->cmdline());

    dst->set_recordexecgraph(src->recordexecgraph());

    dst->set_issgx(src->issgx());

    dst->set_sgxsid(src->sgxsid());
    dst->set_sgxnonce(src->sgxnonce());
    dst->set_sgxtag(src->sgxtag());
    dst->set_sgxpolicy(src->sgxpolicy());
    dst->set_sgxresult(src->sgxresult());

    dst->set_migrationcheckperiod(src->migrationcheckperiod());
}
}
