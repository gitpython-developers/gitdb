#include <Python.h>

static PyObject* connect_deltas(PyObject *self, PyObject *dstreams);
static PyObject* apply_delta(PyObject* self, PyObject* args);

static PyTypeObject DeltaChunkListType;
