#include <faabric/endpoint/Endpoint.h>

using namespace faabric::endpoint;

void _entrypoint(int argc, char* argv[]);

#define FAABRIC_HTTP_MAIN(port) \
   void main(int argc, char* argv[]) {              \
                                                    \
   }                                                \
                                                    \
   void _entrypoint(int argc, char* argv[]) {

