#ifndef SRC_UTIL_MEMORY_UTIL_H_
#define SRC_UTIL_MEMORY_UTIL_H_

#include <stdio.h>
#include <unistd.h>
#include <sys/resource.h>

namespace utils {

/**
 * Returns the peak (maximum so far) resident set size (physical
 * memory use) measured in bytes, or zero if the value cannot be
 * determined on this OS.
 */
size_t get_peak_RSS()
{
    struct rusage rusage;
    getrusage( RUSAGE_SELF, &rusage );

    return (size_t)(rusage.ru_maxrss * 1024L);
}


/**
 * Returns the current resident set size (physical memory use) measured
 * in bytes, or zero if the value cannot be determined on this OS.
 */
size_t get_current_RSS()
{
    long rss = 0L;
    FILE* fp = NULL;
    if ( (fp = fopen( "/proc/self/statm", "r" )) == NULL )
        return (size_t)0L;      /* Can't open? */
    if ( fscanf( fp, "%*s%ld", &rss ) != 1 )
    {
        fclose( fp );
        return (size_t)0L;      /* Can't read? */
    }
    fclose( fp );
    return (size_t)rss * (size_t)sysconf( _SC_PAGESIZE);
}

}
#endif /* SRC_UTIL_MEMORY_UTIL_H_ */