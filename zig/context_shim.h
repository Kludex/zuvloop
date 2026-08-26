#ifndef ZUVLOOP_CONTEXT_SHIM_H
#define ZUVLOOP_CONTEXT_SHIM_H

#include <stddef.h>

enum {
    ZUVLOOP_PYTHREADSTATE_CONTEXT_OFFSET = offsetof(PyThreadState, context),
};

#endif
