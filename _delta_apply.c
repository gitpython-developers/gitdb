#include <Python.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <math.h>
#include <string.h>

typedef unsigned long long ull;
typedef unsigned int uint;
typedef unsigned char uchar;
typedef uchar bool;

// Constants
const ull gDIV_grow_by = 100;


// DELTA INFO
/////////////
typedef struct {
	uint dso;			// delta stream offset
	uint to;			// target offset (cache)
} DeltaInfo;


// DELTA CHUNK 
////////////////
// Internal Delta Chunk Objects
// They are just used to keep information parsed from a stream
// The data pointer is always shared
typedef struct {
	ull to;
	ull ts;
	ull so;
	const uchar* data;
} DeltaChunk;

inline
void DC_init(DeltaChunk* dc, ull to, ull ts, ull so, const uchar* data)
{
	dc->to = to;
	dc->ts = ts;
	dc->so = so;
	dc->data = NULL;
}


inline
ull DC_rbound(const DeltaChunk* dc)
{
	return dc->to + dc->ts;
}

// Apply
// TODO: remove, just left it for reference
inline
void DC_apply(const DeltaChunk* dc, const uchar* base, PyObject* writer, PyObject* tmpargs)
{
	PyObject* buffer = 0;
	if (dc->data){
		buffer = PyBuffer_FromMemory((void*)dc->data, dc->ts);
	} else {
		buffer = PyBuffer_FromMemory((void*)(base + dc->so), dc->ts);
	}

	if (PyTuple_SetItem(tmpargs, 0, buffer)){
		assert(0);
	}
	
	// tuple steals reference, and will take care about the deallocation
	PyObject_Call(writer, tmpargs, NULL);
	
}


// DELTA CHUNK VECTOR
/////////////////////

typedef struct {
	DeltaInfo* mem;						// Memory
	const uchar* dstream;				// pointer to delta stream we index
	Py_ssize_t size;					// Size in DeltaInfos
	Py_ssize_t reserved_size;			// Reserve in DeltaInfos
} DeltaInfoVector;



// Reserve enough memory to hold the given amount of delta chunks
// Return 1 on success
// NOTE: added a minimum allocation to assure reallocation is not done 
// just for a single additional entry. DCVs change often, and reallocs are expensive
inline
int DIV_reserve_memory(DeltaInfoVector* vec, uint num_dc)
{
	if (num_dc <= vec->reserved_size){
		return 1;
	}
	
	if (num_dc - vec->reserved_size < 10){
		num_dc += gDIV_grow_by;
	}
	
#ifdef DEBUG
	bool was_null = vec->mem == NULL;
#endif
	
	if (vec->mem == NULL){
		vec->mem = PyMem_Malloc(num_dc * sizeof(DeltaInfo));
	} else {
		vec->mem = PyMem_Realloc(vec->mem, num_dc * sizeof(DeltaInfo));
	}
	
	if (vec->mem == NULL){
		Py_FatalError("Could not allocate memory for append operation");
	}
	
	vec->reserved_size = num_dc;
	
#ifdef DEBUG
	const char* format = "Allocated %i bytes at %p, to hold up to %i chunks\n";
	if (!was_null)
		format = "Re-allocated %i bytes at %p, to hold up to %i chunks\n";
	fprintf(stderr, format, (int)(vec->reserved_size * sizeof(DeltaInfo)), vec->mem, (int)vec->reserved_size);
#endif
	
	return vec->mem != NULL;
}

/*
Grow the delta chunk list by the given amount of bytes.
This may trigger a realloc, but will do nothing if the reserved size is already
large enough.
Return 1 on success, 0 on failure
*/
inline
int DIV_grow_by(DeltaInfoVector* vec, uint num_dc)
{
	return DIV_reserve_memory(vec, vec->reserved_size + num_dc);
}

int DIV_init(DeltaInfoVector* vec, ull initial_size)
{
	vec->mem = NULL;
	vec->size = 0;
	vec->reserved_size = 0;
	
	return DIV_grow_by(vec, initial_size);
}

inline
ull DIV_len(const DeltaInfoVector* vec)
{
	return vec->size;
}

inline
ull DIV_lbound(const DeltaInfoVector* vec)
{
	assert(vec->size && vec->mem);
	return vec->mem->to;
}

// Return item at index
inline
DeltaInfo* DIV_get(const DeltaInfoVector* vec, Py_ssize_t i)
{
	assert(i < vec->size && vec->mem);
	return &vec->mem[i];
}

// Return last item
inline
DeltaInfo* DIV_last(const DeltaInfoVector* vec)
{
	return DIV_get(vec, vec->size-1);
}

inline
ull DIV_rbound(const DeltaInfoVector* vec)
{
	return DC_rbound(DIV_last(vec)); 
}

inline
ull DIV_size(const DeltaInfoVector* vec)
{
	return DIV_rbound(vec) - DIV_lbound(vec);
}

inline
int DIV_empty(const DeltaInfoVector* vec)
{
	return vec->size == 0;
}

// Return end pointer of the vector
inline
const DeltaInfo* DIV_end(const DeltaInfoVector* vec)
{
	assert(!DIV_empty(vec));
	return vec->mem + vec->size;
}

// return first item in vector
inline
DeltaInfo* DIV_first(const DeltaInfoVector* vec)
{
	assert(!DIV_empty(vec));
	return vec->mem;
}

void DIV_destroy(DeltaInfoVector* vec)
{
	if (vec->mem){
#ifdef DEBUG
		fprintf(stderr, "Freeing %p\n", (void*)vec->mem);
#endif

		const DeltaInfo* end = &vec->mem[vec->size];
		DeltaInfo* i;
		for(i = vec->mem; i < end; i++){
			DC_destroy(i);
		}
		
		PyMem_Free(vec->mem);
		vec->size = 0;
		vec->reserved_size = 0;
		vec->mem = 0;
	}
}

// Reset this vector so that its existing memory can be filled again.
// Memory will be kept, but not cleaned up
inline
void DIV_forget_members(DeltaInfoVector* vec)
{
	vec->size = 0;
}

// Reset the vector so that its size will be zero, and its members will 
// have been deallocated properly.
// It will keep its memory though, and hence can be filled again
inline
void DIV_reset(DeltaInfoVector* vec)
{
	if (vec->size == 0)
		return;
	
	DeltaInfo* dc = DIV_first(vec);
	const DeltaInfo* dcend = DIV_end(vec);
	for(;dc < dcend; dc++){
		DC_destroy(dc);
	}
	
	vec->size = 0;
}


// Append one chunk to the end of the list, and return a pointer to it
// It will not have been initialized !
static inline
DeltaInfo* DIV_append(DeltaInfoVector* vec)
{
	if (vec->size + 1 > vec->reserved_size){
		DIV_grow_by(vec, gDIV_grow_by);
	}
	
	DeltaInfo* next = vec->mem + vec->size; 
	vec->size += 1;
	return next;
}

// Return delta chunk being closest to the given absolute offset
inline
DeltaInfo* DIV_closest_chunk(const DeltaInfoVector* vec, ull ofs)
{
	assert(vec->mem);
	
	ull lo = 0;
	ull hi = vec->size;
	ull mid;
	DeltaInfo* dc;
	
	while (lo < hi)
	{
		mid = (lo + hi) / 2;
		dc = vec->mem + mid;
		if (dc->to > ofs){
			hi = mid;
		} else if ((DC_rbound(dc) > ofs) | (dc->to == ofs)) {
			return dc;
		} else {
			lo = mid + 1;
		}
	}
	
	return DIV_last(vec);
}


// Return the amount of chunks a slice at the given spot would have 
inline
uint DIV_count_slice_chunks(const DeltaInfoVector* src, ull ofs, ull size)
{
	uint num_dc = 0;
	DeltaInfo* cdc = DIV_closest_chunk(src, ofs);
	
	// partial overlap
	if (cdc->to != ofs) {
		const ull relofs = ofs - cdc->to;
		size -= cdc->ts - relofs < size ? cdc->ts - relofs : size;
		num_dc += 1;
		cdc += 1;
		
		if (size == 0){
			return num_dc;
		}
	}
	
	const DeltaInfo* vecend = DIV_end(src);
	for( ;(cdc < vecend) && size; ++cdc){
		num_dc += 1;
		if (cdc->ts < size) {
			size -= cdc->ts;
		} else {
			size = 0;
			break;
		}
	}
	
	return num_dc;
}

// Write a slice as defined by its absolute offset in bytes and its size into the given
// destination memory. The individual chunks written will be a deep copy of the source 
// data chunks
// Return: number of chunks in the slice
inline
uint DIV_copy_slice_to(const DeltaInfoVector* src, DeltaInfo* dest, ull ofs, ull size)
{
	assert(DIV_lbound(src) <= ofs);
	assert((ofs + size) <= DIV_rbound(src));
	
	DeltaInfo* cdc = DIV_closest_chunk(src, ofs);
	uint num_chunks = 0;
	
	// partial overlap
	if (cdc->to != ofs) {
		const ull relofs = ofs - cdc->to;
		DC_offset_copy_to(cdc, dest, relofs, cdc->ts - relofs < size ? cdc->ts - relofs : size);
		cdc += 1;
		size -= dest->ts;
		dest += 1;				// must be here, we are reading the size !
		num_chunks += 1;
		
		if (size == 0){
			return num_chunks;
		}
	}
	
	const DeltaInfo* vecend = DIV_end(src);
	for( ;(cdc < vecend) && size; ++cdc)
	{
		num_chunks += 1;
		if (cdc->ts < size) {
			DC_copy_to(cdc, dest++);
			size -= cdc->ts;
		} else {
			DC_offset_copy_to(cdc, dest, 0, size);
			size = 0;
			break;
		}
	}
	
	assert(size == 0);
	return num_chunks;
}


// Take slices of bdcv into the corresponding area of the tdcv, which is the topmost
// delta to apply.  
bool DIV_connect_with_base(DeltaInfoVector* tdcv, const DeltaInfoVector* bdcv)
{
	uint *const offset_array = PyMem_Malloc(tdcv->size * sizeof(uint));
	if (!offset_array){
		return 0;
	}
	
	uint* pofs = offset_array;
	uint num_addchunks = 0;
	
	DeltaInfo* dc = DIV_first(tdcv);
	const DeltaInfo* dcend = DIV_end(tdcv);
	
	// OFFSET RUN
	for (;dc < dcend; dc++, pofs++)
	{
		// Data chunks don't need processing
		*pofs = num_addchunks;
		if (dc->data){
			continue;
		}
		
		// offset the next chunk by the amount of chunks in the slice
		// - 1, because we replace our own chunk
		num_addchunks += DIV_count_slice_chunks(bdcv, dc->so, dc->ts) - 1;
	}
	
	// reserve enough memory to hold all the new chunks
	// reinit pointers, array could have been reallocated
	DIV_reserve_memory(tdcv, tdcv->size + num_addchunks);
	dc = DIV_last(tdcv);
	dcend = DIV_first(tdcv) - 1;
	
	// now, that we have our pointers with the old size
	tdcv->size += num_addchunks;
	
	// Insert slices, from the end to the beginning, which allows memcpy
	// to be used, with a little help of the offset array
	for (pofs -= 1; dc > dcend; dc--, pofs-- )
	{
		// Data chunks don't need processing
		const uint ofs = *pofs;
		if (dc->data){
			// NOTE: could peek the preceeding chunks to figure out whether they are 
			// all just moved by ofs. In that case, they can move as a whole!
			// tests showed that this is very rare though, even in huge deltas, so its
			// not worth the extra effort
			if (ofs){
				memcpy((void*)(dc + ofs), (void*)dc, sizeof(DeltaInfo));
			}
			continue;
		}
		
		// Copy Chunks, and move their target offset into place
		// As we could override dc when slicing, we get the data here
		const ull relofs = dc->to - dc->so;
		
		DeltaInfo* tdc = dc + ofs;
		DeltaInfo* tdcend = tdc + DIV_copy_slice_to(bdcv, tdc, dc->so, dc->ts);
		for(;tdc < tdcend; tdc++){
			tdc->to += relofs;
		}
	}
	
	PyMem_Free(offset_array);
	return 1;
}

// DELTA CHUNK LIST (PYTHON)
/////////////////////////////
// Internally, it has nothing to do with a ChunkList anymore though
typedef struct {
	PyObject_HEAD
	// -----------
	DeltaInfoVector vec;
	
} DeltaChunkList;


static 
int DCL_init(DeltaChunkList*self, PyObject *args, PyObject *kwds)
{
	if(args && PySequence_Size(args) > 0){
		PyErr_SetString(PyExc_ValueError, "Too many arguments");
		return -1;
	}
	
	DIV_init(&self->vec, 0);
	return 0;
}

static
void DCL_dealloc(DeltaChunkList* self)
{
	DIV_destroy(&(self->vec));
}

static
PyObject* DCL_len(DeltaChunkList* self)
{
	return PyLong_FromUnsignedLongLong(DIV_len(&self->vec));
}

static inline
ull DCL_rbound(DeltaChunkList* self)
{
	if (DIV_empty(&self->vec))
		return 0;
	return DIV_rbound(&self->vec);
}

static
PyObject* DCL_py_rbound(DeltaChunkList* self)
{
	return PyLong_FromUnsignedLongLong(DCL_rbound(self));
}

// Write using a write function, taking remaining bytes from a base buffer
static
PyObject* DCL_apply(DeltaChunkList* self, PyObject* args)
{
	PyObject* pybuf = 0;
	PyObject* writeproc = 0;
	if (!PyArg_ParseTuple(args, "OO", &pybuf, &writeproc)){
		PyErr_BadArgument();
		return NULL;
	}
	
	if (!PyObject_CheckReadBuffer(pybuf)){
		PyErr_SetString(PyExc_ValueError, "First argument must be a buffer-compatible object, like a string, or a memory map");
		return NULL;
	}
	
	if (!PyCallable_Check(writeproc)){
		PyErr_SetString(PyExc_ValueError, "Second argument must be a writer method with signature write(buf)");
		return NULL;
	}
	
	const DeltaChunk* i = self->vec.mem;
	const DeltaChunk* end = DIV_end(&self->vec);
	
	const uchar* data;
	Py_ssize_t dlen;
	PyObject_AsReadBuffer(pybuf, (const void**)&data, &dlen);
	
	PyObject* tmpargs = PyTuple_New(1);
	
	for(; i < end; i++){
		DC_apply(i, data, writeproc, tmpargs);
	}
	
	Py_DECREF(tmpargs);
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
	
	DeltaInfoVector dcv;
	DeltaInfoVector tdcv;
	DIV_init(&dcv, 100);			// should be enough to keep the average text file
	DIV_init(&tdcv, 0);
	
	unsigned int dsi = 0;
	PyObject* ds = 0;
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
		
		// Assume good compression for the adds
		const uint approx_num_cmds = ((dlen / 3) / 10) + (((dlen / 3) * 2) / (2+2+1));
		DIV_reserve_memory(&dcv, approx_num_cmds);
	
		// parse command stream
		ull tbw = 0;							// Amount of target bytes written
		bool is_shared_data = dsi != 0;
		bool is_first_run = dsi == 0;
		
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
					// this really shouldn't happen
					error = 1;
					assert(0);
					break;
				}
				
				DC_init(DIV_append(&dcv), tbw, cp_size, cp_off);
				tbw += cp_size;
				
			} else if (cmd) {
				// Compression reduces fragmentation though, which is why we do it
				// in all cases.
				// It makes the more sense the more consecutive add-chunks we have, 
				// its more likely in big deltas, for big binary files
				const uchar* add_start = data - 1;
				const uchar* add_end = dend;
				ull num_bytes = cmd;
				data += cmd;
				ull num_chunks = 1;
				while (data < dend){
				//while (0){
					const char c = *data;
					if (c & 0x80){
						add_end = data;
						break;
					} else {
						data += 1 + c;			// advance by 1 to skip add cmd
						num_bytes += c;
						num_chunks += 1;
					}
				}
				
				#ifdef DEBUG
				assert(add_end - add_start > 0);
				if (num_chunks > 1){
					fprintf(stderr, "Compression: got %i bytes of %i chunks\n", (int)num_bytes, (int)num_chunks);
				}
				#endif
				
				DeltaChunk* dc = DIV_append(&dcv); 
				DC_init(dc, tbw, num_bytes, 0);
				
				// gather the data, or (possibly) share single blocks
				if (num_chunks > 1){
					uchar* dcdata = PyMem_Malloc(num_bytes);
					while (add_start < add_end){
						const char bytes = *add_start++;
						memcpy((void*)dcdata, (void*)add_start, bytes);
						dcdata += bytes;
						add_start += bytes;
					}
					DC_set_data_with_ownership(dc, dcdata-num_bytes);
				} else {
					DC_set_data(dc, data - cmd, cmd, is_shared_data);
				}
				
				tbw += num_bytes;
			} else {                                                                               
				error = 1;
				PyErr_SetString(PyExc_RuntimeError, "Encountered an unsupported delta cmd: 0");
				goto loop_end;
			}
		}// END handle command opcodes
		if (tbw != target_size){
			PyErr_SetString(PyExc_RuntimeError, "Failed to parse delta stream");
			error = 1;
		}

		if (!is_first_run){
			if (!DIV_connect_with_base(&tdcv, &dcv)){
				error = 1;
			}
		}
		
		#ifdef DEBUG
		fprintf(stderr, "tdcv->size = %i, tdcv->reserved_size = %i\n", (int)tdcv.size, (int)tdcv.reserved_size);
		fprintf(stderr, "dcv->size = %i, dcv->reserved_size = %i\n", (int)dcv.size, (int)dcv.reserved_size);
		#endif
		
		if (is_first_run){
			tdcv = dcv;
			// wipe out dcv without destroying the members, get its own memory
			DIV_init(&dcv, tdcv.size);
		} else {
			// destroy members, but keep memory
			DIV_reset(&dcv);
		}

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
	
	if (dsi > 1){
		// otherwise dcv equals tcl
		DIV_destroy(&dcv);
	}
	
	// Return the actual python object - its just a container
	DeltaChunkList* dcl = DCL_new_instance();
	if (!dcl){
		PyErr_SetString(PyExc_RuntimeError, "Couldn't allocate list");
		// Otherwise tdcv would be deallocated by the chunk list
		DIV_destroy(&tdcv);
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


// Write using a write function, taking remaining bytes from a base buffer
// replaces the corresponding method in python
static
PyObject* apply_delta(PyObject* self, PyObject* args)
{
	PyObject* pybbuf = 0;
	PyObject* pydbuf = 0;
	PyObject* pytbuf = 0;
	if (!PyArg_ParseTuple(args, "OOO", &pybbuf, &pydbuf, &pytbuf)){
		PyErr_BadArgument();
		return NULL;
	}
	
	PyObject* objects[] = { pybbuf, pydbuf, pytbuf };
	assert(sizeof(objects) / sizeof(PyObject*) == 3);
	
	uint i;
	for(i = 0; i < 3; i++){ 
		if (!PyObject_CheckReadBuffer(objects[i])){
			PyErr_SetString(PyExc_ValueError, "Argument must be a buffer-compatible object, like a string, or a memory map");
			return NULL;
		}
	}
	
	Py_ssize_t lbbuf; Py_ssize_t ldbuf; Py_ssize_t ltbuf;
	const uchar* bbuf; const uchar* dbuf;
	uchar* tbuf;
	PyObject_AsReadBuffer(pybbuf, (const void**)(&bbuf), &lbbuf);
	PyObject_AsReadBuffer(pydbuf, (const void**)(&dbuf), &ldbuf);
	
	if (PyObject_AsWriteBuffer(pytbuf, (void**)(&tbuf), &ltbuf)){
		PyErr_SetString(PyExc_ValueError, "Argument 3 must be a writable buffer");
		return NULL;
	}
	
	const uchar* data = dbuf;
	const uchar* dend = dbuf + ldbuf;
	
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
			
			memcpy(tbuf, bbuf + cp_off, cp_size); 
			tbuf += cp_size;
			
		} else if (cmd) {
			memcpy(tbuf, data, cmd);
			tbuf += cmd;
			data += cmd;
		} else {                                                                               
			PyErr_SetString(PyExc_RuntimeError, "Encountered an unsupported delta cmd: 0");
			return NULL;
		}
	}// END handle command opcodes
	
	Py_RETURN_NONE;
}
