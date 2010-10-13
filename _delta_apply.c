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
const ull gDVC_grow_by = 100;

#ifdef DEBUG
#define DBG_check(vec) assert(DCV_dbg_check_integrity(vec))
#else
#define DBG_check(vec)
#endif

// DELTA CHUNK 
////////////////
// Internal Delta Chunk Objects
typedef struct {
	ull to;
	ull ts;
	ull so;
	const uchar* data;
	bool data_shared;
} DeltaChunk;

inline
void DC_init(DeltaChunk* dc, ull to, ull ts, ull so)
{
	dc->to = to;
	dc->ts = ts;
	dc->so = so;
	dc->data = NULL;
	dc->data_shared = 0;
}

inline
void DC_deallocate_data(DeltaChunk* dc)
{
	if (!dc->data_shared && dc->data){
		PyMem_Free((void*)dc->data);
	}
	dc->data = NULL;
}

inline
void DC_destroy(DeltaChunk* dc)
{
	DC_deallocate_data(dc);
}

// Store a copy of data in our instance. If shared is 1, the data will be shared, 
// hence it will only be stored, but the memory will not be touched, or copied.
inline
void DC_set_data(DeltaChunk* dc, const uchar* data, Py_ssize_t dlen, bool shared)
{
	DC_deallocate_data(dc);
	
	if (data == 0){
		dc->data = NULL;
		dc->data_shared = 0;
		return;
	}
	
	dc->data_shared = shared;
	if (shared){
		dc->data = data;
	} else {
		dc->data = (uchar*)PyMem_Malloc(dlen);
		memcpy((void*)dc->data, (void*)data, dlen);
	}
	
}

// Make the given data our own. It is assumed to have the size stored in our instance
// and will be managed by us.
inline
void DC_set_data_with_ownership(DeltaChunk* dc, const uchar* data)
{
	assert(data);
	DC_deallocate_data(dc);
	dc->data = data;
}

inline
ull DC_rbound(const DeltaChunk* dc)
{
	return dc->to + dc->ts;
}

// Apply 
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

// Copy all data from src to dest, the data pointer will be copied too
inline
void DC_copy_to(const DeltaChunk* src, DeltaChunk* dest)
{
	dest->to = src->to;
	dest->ts = src->ts;
	dest->so = src->so;
	dest->data_shared = 0;
	dest->data = NULL;
	
	DC_set_data(dest, src->data, src->ts, 0);
}

// Copy all data with the given offset and size. The source offset, as well
// as the data will be truncated accordingly
inline
void DC_offset_copy_to(const DeltaChunk* src, DeltaChunk* dest, ull ofs, ull size)
{
	assert(size <= src->ts);
	assert(src->to + ofs + size <= DC_rbound(src));
	
	dest->to = src->to + ofs;
	dest->ts = size;
	dest->so = src->so + ofs;
	dest->data = NULL;
	
	if (src->data){
		DC_set_data(dest, src->data + ofs, size, 0);
	} else {
		dest->data_shared = 0;
	}
}


// DELTA CHUNK VECTOR
/////////////////////

typedef struct {
	DeltaChunk* mem;			// Memory
	Py_ssize_t size;					// Size in DeltaChunks
	Py_ssize_t reserved_size;			// Reserve in DeltaChunks
} DeltaChunkVector;



// Reserve enough memory to hold the given amount of delta chunks
// Return 1 on success
// NOTE: added a minimum allocation to assure reallocation is not done 
// just for a single additional entry. DCVs change often, and reallocs are expensive
inline
int DCV_reserve_memory(DeltaChunkVector* vec, uint num_dc)
{
	if (num_dc <= vec->reserved_size){
		return 1;
	}
	
	if (num_dc - vec->reserved_size){
		num_dc += gDVC_grow_by;
	}
	
#ifdef DEBUG
	bool was_null = vec->mem == NULL;
#endif
	
	if (vec->mem == NULL){
		vec->mem = PyMem_Malloc(num_dc * sizeof(DeltaChunk));
	} else {
		vec->mem = PyMem_Realloc(vec->mem, num_dc * sizeof(DeltaChunk));
	}
	
	if (vec->mem == NULL){
		Py_FatalError("Could not allocate memory for append operation");
	}
	
	vec->reserved_size = num_dc;
	
#ifdef DEBUG
	const char* format = "Allocated %i bytes at %p, to hold up to %i chunks\n";
	if (!was_null)
		format = "Re-allocated %i bytes at %p, to hold up to %i chunks\n";
	fprintf(stderr, format, (int)(vec->reserved_size * sizeof(DeltaChunk)), vec->mem, (int)vec->reserved_size);
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
int DCV_grow_by(DeltaChunkVector* vec, uint num_dc)
{
	return DCV_reserve_memory(vec, vec->reserved_size + num_dc);
}

int DCV_init(DeltaChunkVector* vec, ull initial_size)
{
	vec->mem = NULL;
	vec->size = 0;
	vec->reserved_size = 0;
	
	return DCV_grow_by(vec, initial_size);
}

inline
ull DCV_len(const DeltaChunkVector* vec)
{
	return vec->size;
}

inline
ull DCV_lbound(const DeltaChunkVector* vec)
{
	assert(vec->size && vec->mem);
	return vec->mem->to;
}

// Return item at index
inline
DeltaChunk* DCV_get(const DeltaChunkVector* vec, Py_ssize_t i)
{
	assert(i < vec->size && vec->mem);
	return &vec->mem[i];
}

// Return last item
inline
DeltaChunk* DCV_last(const DeltaChunkVector* vec)
{
	return DCV_get(vec, vec->size-1);
}

inline
ull DCV_rbound(const DeltaChunkVector* vec)
{
	return DC_rbound(DCV_last(vec)); 
}

inline
int DCV_empty(const DeltaChunkVector* vec)
{
	return vec->size == 0;
}

// Return end pointer of the vector
inline
const DeltaChunk* DCV_end(const DeltaChunkVector* vec)
{
	assert(!DCV_empty(vec));
	return vec->mem + vec->size;
}

void DCV_destroy(DeltaChunkVector* vec)
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

// Reset this vector so that its existing memory can be filled again.
// Memory will be kept, but not cleaned up
inline
void DCV_forget_members(DeltaChunkVector* vec)
{
	vec->size = 0;
}

// Reset the vector so that its size will be zero, and its members will 
// have been deallocated properly.
// It will keep its memory though, and hence can be filled again
inline
void DCV_reset(DeltaChunkVector* vec)
{
	if (vec->size == 0)
		return;
	
	DeltaChunk* dc = vec->mem;
	const DeltaChunk* dcend = DCV_end(vec);
	for(;dc < dcend; dc++){
		DC_destroy(dc);
	}
	
	vec->size = 0;
}


// Append one chunk to the end of the list, and return a pointer to it
// It will not have been initialized !
static inline
DeltaChunk* DCV_append(DeltaChunkVector* vec)
{
	if (vec->size + 1 > vec->reserved_size){
		DCV_grow_by(vec, gDVC_grow_by);
	}
	
	DeltaChunk* next = vec->mem + vec->size; 
	vec->size += 1;
	return next;
}

// Return delta chunk being closest to the given absolute offset
inline
DeltaChunk* DCV_closest_chunk(const DeltaChunkVector* vec, ull ofs)
{
	assert(vec->mem);
	
	ull lo = 0;
	ull hi = vec->size;
	ull mid;
	DeltaChunk* dc;
	
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
	
	return DCV_last(vec);
}

// Assert the given vector has correct datachunks
// return 1 on success
int DCV_dbg_check_integrity(const DeltaChunkVector* vec)
{
	if(DCV_empty(vec)){
		return 0;
	}
	const DeltaChunk* i = vec->mem;
	const DeltaChunk* end = DCV_end(vec);
	
	ull aparent_size = DCV_rbound(vec) - DCV_lbound(vec);
	ull acc_size = 0;
	for(; i < end; i++){
		acc_size += i->ts;
	}
	if (acc_size != aparent_size)
		return 0;
	
	if (vec->size < 2){
		return 1;
	}
	
	const DeltaChunk* endm1 = DCV_end(vec) - 1;
	for(i = vec->mem; i < endm1; i++){
		const DeltaChunk* n = i+1;
		if (DC_rbound(i) != n->to){
			return 0;
		}
	}
	
	return 1;
}

// Write a slice as defined by its absolute offset in bytes and its size into the given
// destination. The individual chunks written will be a deep copy of the source 
// data chunks
// TODO: this could trigger copying many smallish add-chunk pieces - maybe some sort
// of append-only memory pool would improve performance
inline
void DCV_copy_slice_to(const DeltaChunkVector* src, DeltaChunkVector* dest, ull ofs, ull size)
{
	assert(DCV_lbound(src) <= ofs);
	assert((ofs + size) <= DCV_rbound(src));
	
	DeltaChunk* cdc = DCV_closest_chunk(src, ofs);
	
	// partial overlap
	if (cdc->to != ofs) {
		DeltaChunk* destc = DCV_append(dest);
		const ull relofs = ofs - cdc->to;
		DC_offset_copy_to(cdc, destc, relofs, cdc->ts - relofs < size ? cdc->ts - relofs : size);
		cdc += 1;
		size -= destc->ts;
		
		if (size == 0){
			return;
		}
	}
	
	const DeltaChunk* vecend = DCV_end(src);
	for( ;(cdc < vecend) && size; ++cdc)
	{
		if (cdc->ts < size) {
			DC_copy_to(cdc, DCV_append(dest));
			size -= cdc->ts;
		} else {
			DC_offset_copy_to(cdc, DCV_append(dest), 0, size);
			size = 0;
			break;
		}
	}
	
	assert(size == 0);
}


// Insert all chunks in 'from' to 'to', starting at the delta chunk named 'at' which
// originates in to
// 'at' will be replaced by the items to insert ( special purpose )
// 'at' will be properly destroyed, but all items will just be copied bytewise
// using memcpy. Hence from must just forget about them !
// IMPORTANT: to must have an appropriate size already
inline
void DCV_replace_one_by_many(const DeltaChunkVector* from, DeltaChunkVector* to, DeltaChunk* at)
{
	assert(from->size > 1);
	assert(to->size + from->size - 1 <= to->reserved_size);
	
	// -1 because we replace 'at'
	DC_destroy(at);
	
	// If we are somewhere in the middle, we have to make some space
	if (DCV_last(to) != at) {
		memmove((void*)(at+from->size), (void*)(at+1), (size_t)((DCV_end(to) - (at+1)) * sizeof(DeltaChunk)));
	}
	
	// Finally copy all the items in
	memcpy((void*) at, (void*)from->mem, from->size*sizeof(DeltaChunk));
	
	// FINALLY: update size
	to->size += from->size - 1;
}

// Take slices of bdcv into the corresponding area of the tdcv, which is the topmost
// delta to apply. tmpl is used as temporary space and must be initialzed and destroyed by the 
// caller
void DCV_connect_with_base(DeltaChunkVector* tdcv, const DeltaChunkVector* bdcv, DeltaChunkVector* tmpl)
{
	Py_ssize_t dci = 0;
	Py_ssize_t iend = tdcv->size;
	DeltaChunk* dc; 
	
	DBG_check(tdcv);
	DBG_check(bdcv);
	
	for (;dci < iend; dci++)
	{
		// Data chunks don't need processing
		dc = DCV_get(tdcv, dci);
		if (dc->data){
			continue;
		}
		
		// Copy Chunk Handling
		DCV_copy_slice_to(bdcv, tmpl, dc->so, dc->ts);
		DBG_check(tmpl);
		assert(tmpl->size);
		
		// move target bounds
		DeltaChunk* tdc = tmpl->mem;
		DeltaChunk* tdcend = tmpl->mem + tmpl->size;
		const ull ofs = dc->to - dc->so;
		for(;tdc < tdcend; tdc++){
			tdc->to += ofs;
		}
		
		// insert slice into our list
		if (tmpl->size == 1){
			// Its not data, so destroy is not really required, anyhow ... 
			DC_destroy(dc);
			*dc = *DCV_get(tmpl, 0);
		} else {
			DCV_reserve_memory(tdcv, tdcv->size + tmpl->size - 1 + gDVC_grow_by);
			dc = DCV_get(tdcv, dci);
			DCV_replace_one_by_many(tmpl, tdcv, dc);
			// Compensate for us being replaced
			dci += tmpl->size-1;
			iend += tmpl->size-1;
		}
		
		DBG_check(tdcv);
	
		// make sure the members will not be deallocated by the list
		DCV_forget_members(tmpl);
	}
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
	DCV_destroy(&(self->vec));
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
	return DCV_rbound(&self->vec);
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
	const DeltaChunk* end = DCV_end(&self->vec);
	
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
	
	DeltaChunkVector dcv;
	DeltaChunkVector tdcv;
	DeltaChunkVector tmpl;
	DCV_init(&dcv, 100);			// should be enough to keep the average text file
	DCV_init(&tdcv, 0);
	DCV_init(&tmpl, 200);
	
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
		
		// estimate number of ops - assume one third adds, half two byte (size+offset) copies
		// Assume good compression for the adds
		const uint approx_num_cmds = ((dlen / 3) / 10) + (((dlen / 3) * 2) / (2+2+1));
		DCV_reserve_memory(&dcv, approx_num_cmds);
	
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
				
				DC_init(DCV_append(&dcv), tbw, cp_size, cp_off);
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
				
				DeltaChunk* dc = DCV_append(&dcv); 
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
			DCV_connect_with_base(&tdcv, &dcv, &tmpl);
		}
		
		#ifdef DEBUG
		fprintf(stderr, "tdcv->size = %i, tdcv->reserved_size = %i\n", (int)tdcv.size, (int)tdcv.reserved_size);
		fprintf(stderr, "dcv->size = %i, dcv->reserved_size = %i\n", (int)dcv.size, (int)dcv.reserved_size);
		#endif
		
		if (is_first_run){
			tdcv = dcv;
			// wipe out dcv without destroying the members, get its own memory
			DCV_init(&dcv, tdcv.size);
		} else {
			// destroy members, but keep memory
			DCV_reset(&dcv);
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
	
	DCV_destroy(&tmpl);
	if (dsi > 1){
		// otherwise dcv equals tcl
		DCV_destroy(&dcv);
	}
	
	// Return the actual python object - its just a container
	DeltaChunkList* dcl = DCL_new_instance();
	if (!dcl){
		PyErr_SetString(PyExc_RuntimeError, "Couldn't allocate list");
		// Otherwise tdcv would be deallocated by the chunk list
		DCV_destroy(&tdcv);
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

