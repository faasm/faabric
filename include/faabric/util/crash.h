
namespace faabric::util {

/*
 * Sets up crash handling. By default covers a number of signals that would
 * otherwise cause a crash. Signal argument can be provided to reinstating crash
 * handling for a specific signal after it's been used elsewhere in the
 * application (e.g.  for dirty tracking).
 */
void setUpCrashHandler(int sig = -1);

/*
 * Prints the stack trace for a given signal. Only to be called in signal
 * handlers.
 */
void handleCrash(int sig);

}
