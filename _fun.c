#include <Python.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <math.h>
#include <string.h>

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
typedef unsigned int uint;
typedef unsigned char uchar;

// DELTA CHUNK 
////////////////
// Internal Delta Chunk Objects
typedef struct {
	ull to;
	ull ts;
	ull so;
	uchar* data;
} DeltaChunk;

void DC_init(DeltaChunk* dc, ull to, ull ts, ull so)
{
	dc->to = to;
	dc->ts = ts;
	dc->so = so;
	dc->data = NULL;
}

void DC_destroy(DeltaChunk* dc)
{
	if (dc->data){
		PyMem_Free((void*)dc->data);
	}
}

// Store a copy of data in our instance
void DC_set_data(DeltaChunk* dc, const uchar* data, Py_ssize_t dlen)
{
	if (dc->data){
		PyMem_Free((void*)dc->data);
	}
	
	if (data == 0){
		dc->data = NULL;
		return;
	}
	
	dc->data = (uchar*)PyMem_Malloc(dlen);
	memcpy(dc->data, data, dlen);
}

ull DC_rbound(DeltaChunk* dc)
{
	return dc->to + dc->ts;
}


// DELTA CHUNK VECTOR
/////////////////////

typedef struct {
	DeltaChunk* mem;			// Memory
	Py_ssize_t size;					// Size in DeltaChunks
	Py_ssize_t reserved_size;			// Reserve in DeltaChunks
} DeltaChunkVector;

/*
Grow the delta chunk list by the given amount of bytes.
This may trigger a realloc, but will do nothing if the reserved size is already
large enough.
Return 1 on success, 0 on failure
*/
static
int DCV_grow(DeltaChunkVector* vec, uint num_dc)
{
	const uint grow_by_chunks = (vec->size + num_dc) - vec->reserved_size;  
	if (grow_by_chunks <= 0){
		return 1;
	}
	
	if (vec->mem == NULL){
		vec->mem = PyMem_Malloc(grow_by_chunks * sizeof(DeltaChunk));
	} else {
		vec->mem = PyMem_Realloc(vec->mem, (vec->reserved_size + grow_by_chunks) * sizeof(DeltaChunk));
	}
	
	if (vec->mem == NULL){
		Py_FatalError("Could not allocate memory for append operation");
	}
	
	vec->reserved_size = vec->reserved_size + grow_by_chunks;
	
#ifdef DEBUG
	fprintf(stderr, "Allocated %i bytes at %p, to hold up to %i chunks\n", (int)((vec->reserved_size + grow_by_chunks) * sizeof(DeltaChunk)), vec->mem, (int)(vec->reserved_size + grow_by_chunks));
#endif
	
	return vec->mem != NULL;
}

int DCV_init(DeltaChunkVector* vec, ull initial_size)
{
	vec->mem = NULL;
	vec->size = 0;
	vec->reserved_size = 0;
	
	return DCV_grow(vec, initial_size);
}

static inline
ull DCV_len(DeltaChunkVector* vec)
{
	return vec->size;
}

// Return item at index
static inline
DeltaChunk* DCV_get(DeltaChunkVector* vec, Py_ssize_t i)
{
	assert(i < vec->size && vec->mem);
	return &vec->mem[i];
}

static inline
int DCV_empty(DeltaChunkVector* vec)
{
	return vec->size == 0;
}

// Return end pointer of the vector
static inline
DeltaChunk* DCV_end(DeltaChunkVector* vec)
{
	assert(!DCV_empty(vec));
	return &vec->mem[vec->size];
}

void DCV_dealloc(DeltaChunkVector* vec)
{
	if (vec->mem){
#ifdef DEBUG
		fprintf(stderr, "Freeing %p\n", (void*)vec->mem);
#endif

		const DeltaChunk* end = &vec->mem[vec->size];
		DeltaChunk* i;
		for(i = vec->mem; i < end; i++){
			DC_destroy(i);
		}
		
		PyMem_Free(vec->mem);
		vec->size = 0;
		vec->reserved_size = 0;
		vec->mem = 0;
	}
}

// Append num-chunks to the end of the list, possibly reallocating existing ones
// Return a pointer to the first of the added items. They are already null initialized
// If num-chunks == 0, it returns the end pointer of the allocated memory
static inline
DeltaChunk* DCV_append_multiple(DeltaChunkVector* vec, uint num_chunks)
{
	if (vec->size + num_chunks > vec->reserved_size){
		DCV_grow(vec, (vec->size + num_chunks) - vec->reserved_size);
	}
	Py_FatalError("Could not allocate memory for append operation");
	Py_ssize_t old_size = vec->size;
	vec->size += num_chunks;
	
	for(;old_size < vec->size; ++old_size){
		DC_init(DCV_get(vec, old_size), 0, 0, 0);
	}
	
	return &vec->mem[old_size];
}

// Append one chunk to the end of the list, and return a pointer to it
// It will have been initialized.
static inline
DeltaChunk* DCV_append(DeltaChunkVector* vec)
{
	if (vec->size + 1 > vec->reserved_size){
		DCV_grow(vec, 1);
	}
	
	DeltaChunk* next = vec->mem + vec->size; 
	vec->size += 1;
	return next;
}

// DELTA CHUNK LIST (PYTHON)
/////////////////////////////

typedef struct {
	PyObject_HEAD
	// -----------
	DeltaChunkVector vec;
	
} DeltaChunkList;


static 
int DCL_init(DeltaChunkList*self, PyObject *args, PyObject *kwds)
{
	if(args && PySequence_Size(args) > 0){
		PyErr_SetString(PyExc_ValueError, "Too many arguments");
		return -1;
	}
	
	DCV_init(&self->vec, 0);
	return 0;
}

static
void DCL_dealloc(DeltaChunkList* self)
{
	DCV_dealloc(&(self->vec));
}

static
PyObject* DCL_len(DeltaChunkList* self)
{
	return PyLong_FromUnsignedLongLong(DCV_len(&self->vec));
}

static inline
ull DCL_rbound(DeltaChunkList* self)
{
	if (DCV_empty(&self->vec))
		return 0;
	return DC_rbound(DCV_get(&self->vec, self->vec.size - 1));
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
	(initproc)DCL_init,						/* tp_init */
	0,						/* tp_alloc */
	0,		/* tp_new */
};


// Makes a new copy of the DeltaChunkList - you have to do everything yourselve
// in C ... want C++ !!
DeltaChunkList* DCL_new_instance(void)
{
	DeltaChunkList* dcl = (DeltaChunkList*) PyType_GenericNew(&DeltaChunkListType, 0, 0);
	assert(dcl);
	
	DCL_init(dcl, 0, 0);
	assert(dcl->vec.size == 0);
	assert(dcl->vec.mem == NULL);
	return dcl;
}

inline
ull msb_size(const uchar** datap, const uchar* top)
{
	const uchar *data = *datap;
	ull cmd, size = 0;
	uint i = 0;
	do {
		cmd = *data++;
		size |= (cmd & 0x7f) << i;
		i += 7;
	} while (cmd & 0x80 && data < top);
	*datap = data;
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
	
	DeltaChunkVector bdcv;
	DeltaChunkVector tdcv;
	DeltaChunkVector dcv;
	DCV_init(&bdcv, 0);
	DCV_init(&dcv, 0);
	DCV_init(&tdcv, 0);
	
	
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
		
		const uchar* data;
		Py_ssize_t dlen;
		PyObject_AsReadBuffer(db, (const void**)&data, &dlen);
		const uchar* dend = data + dlen;
		
		// read header
		const ull base_size = msb_size(&data, dend);
		const ull target_size = msb_size(&data, dend);
		
		// estimate number of ops - assume one third adds, half two byte (size+offset) copies
		const uint approx_num_cmds = (dlen / 3) + (((dlen / 3) * 2) / (2+2+1));
		DCV_grow(&dcv, approx_num_cmds);
	
		// parse command stream
		ull tbw = 0;							// Amount of target bytes written
		assert(data < dend);
		while (data < dend)
		{
			const char cmd = *data++;
			
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
					break;
				}
				
				DC_init(DCV_append(&dcv), tbw, cp_size, cp_off);
				tbw += cp_size;
				
			} else if (cmd) {
				// TODO: Compress nodes by parsing them in advance
				DeltaChunk* dc = DCV_append(&dcv); 
				DC_init(dc, tbw, cmd, 0);
				DC_set_data(dc, data, cmd);
				tbw += cmd;
				data += cmd;
			} else {
				error = 1;
				PyErr_SetString(PyExc_RuntimeError, "Encountered an unsupported delta cmd: 0");
				goto loop_end;
			}
		}// END handle command opcodes
		assert(tbw == target_size);
		
		// swap the vector
		// Skip the first vector, as it is also used as top chunk vector
		if (bdcv.mem != tdcv.mem){
			DCV_dealloc(&bdcv);
		}
		bdcv = dcv;
		if (dsi == 0){
			tdcv = dcv;
		}
		DCV_init(&dcv, 0);
		

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
	
	DCV_dealloc(&bdcv);
	if (dsi > 1){
		// otherwise dcv equals tcl
		DCV_dealloc(&dcv);
	}
	
	// Return the actual python object - its just a container
	DeltaChunkList* dcl = DCL_new_instance();
	if (!dcl){
		PyErr_SetString(PyExc_RuntimeError, "Couldn't allocate list");
		// Otherwise tdcv would be deallocated by the chunk list
		DCV_dealloc(&tdcv);
		error = 1;
	} else {
		// Plain copy, don't deallocate
		dcl->vec = tdcv;
	}
	
	if (error){
		// Will dealloc tdcv
		Py_XDECREF(dcl);
		return NULL;
	}
	
	return (PyObject*)dcl;
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

	if (PyType_Ready(&DeltaChunkListType) < 0)
		return;
	
	m = Py_InitModule3("_fun", py_fun, NULL);
	if (m == NULL)
		return;
	
	Py_INCREF(&DeltaChunkListType);
	PyModule_AddObject(m, "Noddy", (PyObject *)&DeltaChunkListType);
}
