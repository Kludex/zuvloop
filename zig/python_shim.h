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
#include <Python.h>

typedef struct {
    uintptr_t storage[2];
} zuvloop_critical_section;

void zuvloop_critical_section_begin(zuvloop_critical_section *section);
void zuvloop_critical_section_end(zuvloop_critical_section *section);
void zuvloop_Py_INCREF(PyObject *object);
void zuvloop_Py_DECREF(PyObject *object);
int zuvloop_PyBytes_Check(PyObject *object);
int zuvloop_PyBytes_CheckExact(PyObject *object);
int zuvloop_PyLong_Check(PyObject *object);
int zuvloop_PyTuple_Check(PyObject *object);
int zuvloop_PyUnicode_Check(PyObject *object);

#endif
