#ifndef ZUVLOOP_PYTHON_SHIM_H
#define ZUVLOOP_PYTHON_SHIM_H

#if defined(__MINGW32__) && defined(__aarch64__)
// MinGW omits the ARM64 intrinsic that CPython uses to read its x18 thread pointer.
static inline unsigned long long zuvloop_read_x18(void) {
    unsigned long long value;
    __asm__("mov %0, x18" : "=r"(value));
    return value;
}
#define __getReg(reg) zuvloop_read_x18()
#endif

#if defined(__GNUC__) || defined(__clang__)
// Zig rejects C11 alignments below the type's natural alignment; this equivalent form never reduces it.
#define _Py_ALIGNED_DEF(N, T) __attribute__((aligned(N))) T
#endif
#include <Python.h>

// TODO: drop when 3.12 support ends. These public names arrived in 3.13.
#if PY_VERSION_HEX < 0x030D0000
typedef _PyTime_t PyTime_t;
static inline int PyTime_MonotonicRaw(PyTime_t *result) {
    *result = _PyTime_GetMonotonicClock();
    return 0;
}
static inline int PyObject_VisitManagedDict(PyObject *object, visitproc visit, void *arg) {
    return _PyObject_VisitManagedDict(object, visit, arg);
}
static inline void PyObject_ClearManagedDict(PyObject *object) {
    _PyObject_ClearManagedDict(object);
}
#define Py_CONSTANT_NONE 0
#define Py_CONSTANT_FALSE 1
#define Py_CONSTANT_TRUE 2
static inline PyObject *Py_GetConstantBorrowed(unsigned int constant_id) {
    return constant_id == Py_CONSTANT_NONE ? Py_None : constant_id == Py_CONSTANT_TRUE ? Py_True : Py_False;
}
#endif

typedef struct {
    uintptr_t storage[2];
} zuvloop_critical_section;

// Thread CPU nanoseconds, or -1 if the clock is unavailable.
int64_t zuvloop_thread_cpu_time(void);

void zuvloop_critical_section_begin(zuvloop_critical_section *section, PyObject *object);
void zuvloop_critical_section_end(zuvloop_critical_section *section);
PyObject *zuvloop_PyModuleDef_Init(PyModuleDef *definition);
void zuvloop_Py_INCREF(PyObject *object);
void zuvloop_Py_DECREF(PyObject *object);
int zuvloop_PyBytes_Check(PyObject *object);
int zuvloop_PyBytes_CheckExact(PyObject *object);
int zuvloop_PyLong_Check(PyObject *object);
int zuvloop_PyTuple_Check(PyObject *object);
int zuvloop_PyUnicode_Check(PyObject *object);

#endif
