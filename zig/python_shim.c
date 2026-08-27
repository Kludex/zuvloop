#include "python_shim.h"

#ifdef Py_GIL_DISABLED
static PyMutex zuvloop_mutex;
#endif

void zuvloop_critical_section_begin(zuvloop_critical_section *section) {
#ifdef Py_GIL_DISABLED
    _Static_assert(sizeof(*section) >= sizeof(PyCriticalSection), "critical section storage is too small");
    PyCriticalSection_BeginMutex((PyCriticalSection *)section, &zuvloop_mutex);
#else
    (void)section;
#endif
}

void zuvloop_critical_section_end(zuvloop_critical_section *section) {
#ifdef Py_GIL_DISABLED
    PyCriticalSection_End((PyCriticalSection *)section);
#else
    (void)section;
#endif
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
