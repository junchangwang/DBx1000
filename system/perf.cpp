/* ---------------------------------------------------------------------------
 **
 ** Author: Junchang Wang @ NUPT
 **
 ** -------------------------------------------------------------------------*/

#include <cstdio>
#include <cassert>
#include <signal.h>
#include <cstring>

int gen_perf_process(char *tag) {
    int perf_pid = fork();
    if (perf_pid > 0) {
            // parent
	    return perf_pid;
    } else if (perf_pid == 0) {
            // child
            perf_pid = getppid();
            char perf_pid_opt[24];
	    memset(perf_pid_opt, 0, 24);
            snprintf(perf_pid_opt, 24, "%d", perf_pid);
            char output_filename[36];
	    memset(output_filename, 0, 36);
            snprintf(output_filename, 36, "perf.output.%s.%d", tag, perf_pid);
            char const *perfargs[12] = {"perf", "stat", "-e",
                    "cache-references,cache-misses,cycles,instructions,branches,branch-misses,page-faults,cpu-migrations", "-p",
                    perf_pid_opt, "-o", output_filename, "--append", NULL};
            execvp("perf", (char **)perfargs);
            assert(0 && "perf failed to start");
    } else {
            perror("fork did not.");
    }   

    assert(0);
    return -1;
}
