#include <Python.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>

static PyObject *PackIndexFile_sha_to_index(PyObject *self, PyObject *args)
{
	const unsigned char *sha;
	const unsigned int sha_len;
	
	// Note: self is only set if we are a c type. We emulate an instance method, 
	// hence we have to get the instance as 'first' argument
	
	// get instance and sha
	PyObject* inst = 0;
	if (!PyArg_ParseTuple(args, "Os#", &inst, &sha, &sha_len))
		return NULL;
	
	if (sha_len != 20) {
		PyErr_SetString(PyExc_ValueError, "Sha is not 20 bytes long");
		return NULL;
	}
	
	if( !inst){
		PyErr_SetString(PyExc_ValueError, "Cannot be called without self");
		return NULL;
	}
	
	// read lo and hi bounds
	PyObject* fanout_table = PyObject_GetAttrString(inst, "_fanout_table");
	if (!fanout_table){
		PyErr_SetString(PyExc_ValueError, "Couldn't obtain fanout table");
		return NULL;
	}
	
	unsigned int lo = 0, hi = 0;
	if (sha[0]){
		PyObject* item = PySequence_GetItem(fanout_table, (const Py_ssize_t)(sha[0]-1));
		lo = PyInt_AS_LONG(item);
		Py_DECREF(item);
	}
	PyObject* item = PySequence_GetItem(fanout_table, (const Py_ssize_t)sha[0]);
	hi = PyInt_AS_LONG(item);
	Py_DECREF(item);
	item = 0;
	
	Py_DECREF(fanout_table);
	
	// get sha query function
	PyObject* get_sha = PyObject_GetAttrString(inst, "sha");
	if (!get_sha){
		PyErr_SetString(PyExc_ValueError, "Couldn't obtain sha method");
		return NULL;
	}
	
	PyObject *sha_str = 0;
	while (lo < hi) {
		const int mid = (lo + hi)/2;
		sha_str = PyObject_CallFunction(get_sha, "i", mid);
		if (!sha_str) {
			return NULL;
		}
		
		// we really trust that string ... for speed 
		const int cmp = memcmp(PyString_AS_STRING(sha_str), sha, 20);
		Py_DECREF(sha_str);
		sha_str = 0;
		
		if (cmp < 0){
			lo = mid + 1;
		}
		else if (cmp > 0) {
			hi = mid;
		}
		else {
			Py_DECREF(get_sha);
			return PyInt_FromLong(mid);
		}// END handle comparison
	}// END while lo < hi
	
	// nothing found, cleanup
	Py_DECREF(get_sha);
	Py_RETURN_NONE;
}


typedef unsigned long long ull;

// Internal Delta Chunk Objects
typedef struct {
	ull to;
	ull ts;
	ull so;
	PyObject* data;
	
	void* next;
} DeltaChunk;


void DC_init(DeltaChunk* dc, ull to, ull ts, ull so, PyObject* data, DeltaChunk* next)
{
	dc->to = to;
	dc->ts = ts;
	dc->so = so;
	Py_XINCREF(data);
	dc->data = data;
	
	dc->next = next;
}

void DC_destroy(DeltaChunk* dc)
{
	Py_XDECREF(dc->data);
}

typedef struct {
	PyObject_HEAD
	// -----------
	DeltaChunk* mem;
	ull size;
	ull reserved_size;
	
} DeltaChunkList;

ull DC_rbound(DeltaChunk* dc)
{
	return dc->to + dc->ts;
}

static
int DCL_new(DeltaChunkList* self, PyObject* args, PyObject* kwds)
{
	self->mem = NULL;			// Memory
	self->size = 0;				// Size in DeltaChunks
	self->reserved_size = 0;	// Reserve in DeltaChunks
	return 1;
}

/*
Grow the delta chunk list by the given amount of bytes.
This may trigger a realloc, but will do nothing if the reserved size is already
large enough.
Return 1 on success, 0 on failure
*/
static
int DCL_grow(DeltaChunkList* self, ull num_dc)
{
	const ull grow_by_chunks = (self->size + num_dc) - self->reserved_size;  
	if (grow_by_chunks <= 0){
		return 1;
	}
	
	if (self->mem){
		self->mem = PyMem_Malloc(grow_by_chunks*sizeof(DeltaChunk));
	} else {
		self->mem = PyMem_Realloc(self->mem, (self->size + grow_by_chunks)*sizeof(DeltaChunk));
	}
	
	return self->mem != NULL;
}


static 
int DCL_init(DeltaChunkList *self, PyObject *args, PyObject *kwds)
{
	if(PySequence_Size(args) > 1){
		PyErr_SetString(PyExc_ValueError, "Zero or one arguments are allowed, providing the initial size of the queue in DeltaChunks");
		return 0;
	}
	
	ull init_size = 0;
	PyArg_ParseTuple(args, "K", &init_size);
	if (init_size == 0){
		init_size = 125000;
	}
	
	return DCL_grow(self, init_size);
}

static
void DCL_dealloc(DeltaChunkList* self)
{
	// TODO: deallocate linked list
	if (self->mem){
		PyMem_Free(self->mem);
		self->size = 0;
		self->reserved_size = 0;
		self->mem = 0;
	}
}

static
PyObject* DCL_len(PyObject* self)
{
	return PyLong_FromUnsignedLongLong(0);
}

static
inline
ull DCL_rbound(DeltaChunkList* self)
{
	if (!self->mem | !self->size)
		return 0;
	return DC_rbound(&(self->mem[self->size-1]));
}

static
PyObject* DCL_py_rbound(DeltaChunkList* self)
{
	return PyLong_FromUnsignedLongLong(DCL_rbound(self));
}

static
PyObject* DCL_apply(PyObject* self, PyObject* args)
{
	
	Py_RETURN_NONE;
}



static PyMethodDef DCL_methods[] = {
    {"apply", (PyCFunction)DCL_apply, METH_VARARGS, "Apply the given iterable of delta streams" },
    {"__len__", (PyCFunction)DCL_len, METH_NOARGS, NULL},
    {"rbound", (PyCFunction)DCL_py_rbound, METH_NOARGS, NULL},
    {NULL}  /* Sentinel */
};

static PyTypeObject DeltaChunkListType = {
	PyObject_HEAD_INIT(NULL)
	0,						   /*ob_size*/
	"DeltaChunkList",			/*tp_name*/
	sizeof(DeltaChunkList),	 /*tp_basicsize*/
	0,						   /*tp_itemsize*/
	(destructor)DCL_dealloc,   /*tp_dealloc*/
	0,						   /*tp_print*/
	0,						   /*tp_getattr*/
	0,						   /*tp_setattr*/
	0,						   /*tp_compare*/
	0,						   /*tp_repr*/
	0,						   /*tp_as_number*/
	0,						   /*tp_as_sequence*/
	0,						   /*tp_as_mapping*/
	0,						   /*tp_hash */
	0,						   /*tp_call*/
	0,						   /*tp_str*/
	0,						   /*tp_getattro*/
	0,						   /*tp_setattro*/
	0,						   /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT,		   /*tp_flags*/
	"Minimal Delta Chunk List",/* tp_doc */
	0,					   /* tp_traverse */
	0,					   /* tp_clear */
	0,					   /* tp_richcompare */
	0,					   /* tp_weaklistoffset */
	0,					   /* tp_iter */
	0,					   /* tp_iternext */
	DCL_methods,			   /* tp_methods */
	0,			   				/* tp_members */
	0,						   /* tp_getset */
	0,						   /* tp_base */
	0,						   /* tp_dict */
	0,						   /* tp_descr_get */
	0,						   /* tp_descr_set */
	0,						   /* tp_dictoffset */
	(initproc)DCL_init,		/* tp_init */
	0,						/* tp_alloc */
	(newfunc)DCL_new,		/* tp_new */
};


static inline
ull msb_size(const char* data, Py_ssize_t dlen, Py_ssize_t offset, Py_ssize_t* out_bytes_read){
	ull size = 0;
	Py_ssize_t i = 0;
	const char* dend = data + dlen;
	for (data = data + offset; data < dend; data+=1, i+=1){
		char c = *data;
		size |= (c & 0x7f) << i*7;
		if (!(c & 0x80)){
			break;
		}
	}// END while in range
	
	*out_bytes_read = i+offset;
	assert((*out_bytes_read * 8) - (*out_bytes_read - 1) <= sizeof(ull));
	return size;
}

static PyObject* connect_deltas(PyObject *self, PyObject *dstreams)
{
	// obtain iterator
	PyObject* stream_iter = 0;
	if (!PyIter_Check(dstreams)){
		stream_iter = PyObject_GetIter(dstreams);
		if (!stream_iter){
			PyErr_SetString(PyExc_RuntimeError, "Couldn't obtain iterator for streams");
			return NULL;
		}
	} else {
		stream_iter = dstreams;
	}
	
	DeltaChunkList* bdcl = 0;
	DeltaChunkList* tdcl = 0;
	DeltaChunkList* dcl = 0;
	
	dcl = tdcl = PyObject_New(DeltaChunkList, &DeltaChunkListType);
	if (!dcl){
		PyErr_SetString(PyExc_RuntimeError, "Couldn't allocate list");
		return NULL;
	}
	
	unsigned int dsi;
	PyObject* ds;
	int error = 0;
	for (ds = PyIter_Next(stream_iter), dsi = 0; ds != NULL; ++dsi, ds = PyIter_Next(stream_iter))
	{
		PyObject* db = PyObject_CallMethod(ds, "read", 0);
		if (!PyObject_CheckReadBuffer(db)){
			error = 1;
			PyErr_SetString(PyExc_RuntimeError, "Returned buffer didn't support the buffer protocol");
			goto loop_end;
		}
		
		const char* data;
		Py_ssize_t dlen;
		PyObject_AsReadBuffer(db, (const void**)&data, &dlen);
		
		// read header
		Py_ssize_t ofs = 0;
		const ull base_size = msb_size(data, dlen, 0, &ofs);
		const ull target_size = msb_size(data, dlen, ofs, &ofs);
	
		// parse command stream
		const char* dend = data + dlen;
		ull tbw = 0;							// Amount of target bytes written
		for (data = data + ofs; data < dend; ++data)
		{
			const char cmd = *data;
			
			if (cmd & 0x80) 
			{
				unsigned long cp_off = 0, cp_size = 0;
				if (cmd & 0x01) cp_off = *data++;
				if (cmd & 0x02) cp_off |= (*data++ << 8);
				if (cmd & 0x04) cp_off |= (*data++ << 16);
				if (cmd & 0x08) cp_off |= ((unsigned) *data++ << 24);
				if (cmd & 0x10) cp_size = *data++;
				if (cmd & 0x20) cp_size |= (*data++ << 8);
				if (cmd & 0x40) cp_size |= (*data++ << 16);
				if (cp_size == 0) cp_size = 0x10000;
				
				const unsigned long rbound = cp_off + cp_size; 
				if (rbound < cp_size ||
					rbound > base_size){
					goto loop_end;
				}
				
				// TODO: Add node
				tbw += cp_size;
				
			} else if (cmd) {
				// TODO: Add node
				tbw += cmd;
			} else {
				error = 1;
				PyErr_SetString(PyExc_RuntimeError, "Encountered an unsupported delta cmd: 0");
				goto loop_end;
			}
		}// END handle command opcodes
		
		assert(tbw == target_size);

loop_end:
		// perform cleanup
		Py_DECREF(ds);
		Py_DECREF(db);
		
		if (error){
			break;
		}
	}// END for each stream object
	
	if (dsi == 0 && ! error){
		PyErr_SetString(PyExc_ValueError, "No streams provided");
	}
	
	if (stream_iter != dstreams){
		Py_DECREF(stream_iter);
	}
	
	if (error){
		return NULL;
	}
	
	return (PyObject*)tdcl;
}

static PyMethodDef py_fun[] = {
	{ "PackIndexFile_sha_to_index", (PyCFunction)PackIndexFile_sha_to_index, METH_VARARGS, "TODO" },
	{ "connect_deltas", (PyCFunction)connect_deltas, METH_O, "TODO" },
	{ NULL, NULL, 0, NULL }
};

#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC init_fun(void)
{
	PyObject *m;

	DeltaChunkListType.tp_new = PyType_GenericNew;
	if (PyType_Ready(&DeltaChunkListType) < 0)
		return;
	
	m = Py_InitModule3("_fun", py_fun, NULL);
	if (m == NULL)
		return;
	
	Py_INCREF(&DeltaChunkListType);
	PyModule_AddObject(m, "Noddy", (PyObject *)&DeltaChunkListType);
}
