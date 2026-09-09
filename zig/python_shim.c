#include "python_shim.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <time.h>
#endif

int64_t zuvloop_thread_cpu_time(void) {
#ifdef _WIN32
    FILETIME created, exited, kernel, user;
    if (!GetThreadTimes(GetCurrentThread(), &created, &exited, &kernel, &user)) {
        return -1;
    }
    uint64_t kernel_ticks = ((uint64_t)kernel.dwHighDateTime << 32) | kernel.dwLowDateTime;
    uint64_t user_ticks = ((uint64_t)user.dwHighDateTime << 32) | user.dwLowDateTime;
    return (int64_t)((kernel_ticks + user_ticks) * 100);
#else
    struct timespec value;
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value) != 0) {
        return -1;
    }
    return (int64_t)value.tv_sec * 1000000000 + value.tv_nsec;
#endif
}

void zuvloop_critical_section_begin(zuvloop_critical_section *section, PyObject *object) {
#ifdef Py_GIL_DISABLED
    _Static_assert(sizeof(*section) >= sizeof(PyCriticalSection), "critical section storage is too small");
    PyCriticalSection_Begin((PyCriticalSection *)section, object);
#else
    (void)section;
    (void)object;
#endif
}

void zuvloop_critical_section_end(zuvloop_critical_section *section) {
#ifdef Py_GIL_DISABLED
    PyCriticalSection_End((PyCriticalSection *)section);
#else
    (void)section;
#endif
}

PyObject *zuvloop_PyModuleDef_Init(PyModuleDef *definition) {
    if (Py_TYPE((PyObject *)&definition->m_base) == NULL) {
        PyModuleDef_Base initial = PyModuleDef_HEAD_INIT;
        definition->m_base = initial;
    }
    return PyModuleDef_Init(definition);
}

void zuvloop_Py_INCREF(PyObject *object) {
    Py_INCREF(object);
}

void zuvloop_Py_DECREF(PyObject *object) {
    Py_DECREF(object);
}

int zuvloop_PyBytes_Check(PyObject *object) {
    return PyBytes_Check(object);
}

int zuvloop_PyBytes_CheckExact(PyObject *object) {
    return PyBytes_CheckExact(object);
}

int zuvloop_PyLong_Check(PyObject *object) {
    return PyLong_Check(object);
}

int zuvloop_PyTuple_Check(PyObject *object) {
    return PyTuple_Check(object);
}

int zuvloop_PyUnicode_Check(PyObject *object) {
    return PyUnicode_Check(object);
}
