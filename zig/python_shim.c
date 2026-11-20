#include "python_shim.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <time.h>
#endif

#if PY_VERSION_HEX < 0x030D0000
int PyTime_MonotonicRaw(PyTime_t *result) {
    *result = _PyTime_GetMonotonicClock();
    return 0;
}

#if PY_VERSION_HEX < 0x030C0000
// 3.11 types carry an ordinary `__dict__` slot, which `_PyObject_GetDictPtr` locates.
int PyObject_VisitManagedDict(PyObject *object, visitproc visit, void *arg) {
    PyObject **dict = _PyObject_GetDictPtr(object);
    if (dict != NULL) {
        Py_VISIT(*dict);
    }
    return 0;
}

void PyObject_ClearManagedDict(PyObject *object) {
    PyObject **dict = _PyObject_GetDictPtr(object);
    if (dict != NULL) {
        Py_CLEAR(*dict);
    }
}

void *PyObject_GetItemData(PyObject *object) {
    return (char *)object + Py_TYPE(object)->tp_basicsize;
}

PyObject *PyErr_GetRaisedException(void) {
    PyObject *type, *value, *traceback;
    PyErr_Fetch(&type, &value, &traceback);
    if (type == NULL) {
        return NULL;
    }
    PyErr_NormalizeException(&type, &value, &traceback);
    if (traceback != NULL) {
        PyException_SetTraceback(value, traceback);
        Py_DECREF(traceback);
    }
    Py_DECREF(type);
    return value;
}

void PyErr_SetRaisedException(PyObject *exception) {
    PyObject *type = (PyObject *)Py_TYPE(exception);
    Py_INCREF(type);
    PyErr_Restore(type, exception, PyException_GetTraceback(exception));
}
#else
int PyObject_VisitManagedDict(PyObject *object, visitproc visit, void *arg) {
    return _PyObject_VisitManagedDict(object, visit, arg);
}

void PyObject_ClearManagedDict(PyObject *object) {
    _PyObject_ClearManagedDict(object);
}
#endif

PyObject *Py_GetConstantBorrowed(unsigned int constant_id) {
    switch (constant_id) {
    case Py_CONSTANT_NONE:
        return Py_None;
    case Py_CONSTANT_FALSE:
        return Py_False;
    case Py_CONSTANT_TRUE:
        return Py_True;
    default:
        PyErr_BadInternalCall();
        return NULL;
    }
}
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
