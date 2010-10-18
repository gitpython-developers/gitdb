#include <Python.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <math.h>
#include <string.h>

typedef unsigned long long ull;
typedef unsigned int uint;
typedef unsigned char uchar;
typedef unsigned short ushort;
typedef uchar bool;

// Constants
const ull gDIV_grow_by = 100;


// DELTA STREAM ACCESS
///////////////////////
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


// TOP LEVEL STREAM INFO
/////////////////////////////
typedef struct {
	const uchar			*tds;					// Toplevel delta stream
	const uchar			*cstart;				// start of the chunks
	Py_ssize_t			 tdslen;				// size of tds in bytes
	Py_ssize_t 			 target_size;			// size of the target buffer which can hold all data
	uint				 num_chunks;			// amount of chunks in the delta stream
	PyObject			*parent_object;
} ToplevelStreamInfo;


void TSI_init(ToplevelStreamInfo* info)
{
	info->tds = NULL;
	info->cstart = NULL;
	info->tdslen = 0;
	info->num_chunks = 0;
	info->target_size = 0;
	info->parent_object = 0;
}

void TSI_destroy(ToplevelStreamInfo* info)
{
	if (info->parent_object){
		Py_DECREF(info->parent_object);
		info->parent_object = 0;
	} else if (info->tds){
		PyMem_Free((void*)info->tds);
	}
}

inline
const uchar* TSI_end(ToplevelStreamInfo* info)
{
	return info->tds + info->tdslen;
}

inline
const uchar* TSI_first(ToplevelStreamInfo* info)
{
	return info->cstart;
}

// set the stream, and initialize it
// initialize our set stream to point to the first chunk
// Fill in the header information, which is the base and target size
inline
void TSI_set_stream(ToplevelStreamInfo* info, const uchar* stream)
{
	info->tds = stream;
	info->cstart = stream;
	
	assert(info->tds && info->tdslen);
	
	// init stream
	const uchar* tdsend = TSI_end(info);
	msb_size(&info->cstart, tdsend);							// base size
	info->target_size = msb_size(&info->cstart, tdsend);
}



// duplicate the data currently owned by the parent object drop its refcount
// return 1 on success
bool TSI_copy_stream_from_object(ToplevelStreamInfo* info)
{
	assert(info->parent_object);
	
	uchar* ptmp = PyMem_Malloc(info->tdslen);
	if (!ptmp){
		return 0;
	}
	uint ofs = (uint)(info->cstart - info->tds);
	memcpy((void*)ptmp, info->tds, info->tdslen);
	
	info->tds = ptmp;
	info->cstart = ptmp + ofs;
	
	Py_DECREF(info->parent_object);
	info->parent_object = 0;
	
	return 1;
}

// make sure we have the given amount of memory available. This will change 
// our official length in bytes right away, its up to the caller 
// to do something useful with the freed space
// Return true on success
bool TSI_resize(ToplevelStreamInfo* info, uint num_bytes)
{
	assert(info->tds);
	if (num_bytes <= info->tdslen){
		return 1;
	}
	uint ofs = (uint)(info->cstart - info->tds);
	info->tds = PyMem_Realloc((void*)info->tds, num_bytes);
	info->tdslen = num_bytes;
	info->cstart = info->tds + ofs;
	
	return info->tds != NULL;
}

// DELTA CHUNK 
////////////////
// Internal Delta Chunk Objects
// They are just used to keep information parsed from a stream
// The data pointer is always shared
typedef struct {
	ull to;
	uint ts;
	uint so;
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

// Encode the information in the given delta chunk and write the byte-stream
// into the given output stream
// It will be copied into the given bounds, the given size must be the final size
// and work with the given relative offset - hence the bounds are assumed to be 
// correct and to fit within the unaltered dc
inline
void DC_encode_to(const DeltaChunk* dc, uchar** pout, uint ofs, uint size)
{
	uchar* out = *pout;
	if (dc->data){
		*out++ = (uchar)size;
		memcpy(out, dc->data+ofs, size);
		out += size;
	} else {
		uchar i = 0x80;
		uchar* op = out++;
		uint moff = dc->so+ofs;
		
		if (moff & 0x000000ff)
			*out++ = moff >> 0,  i |= 0x01;
		if (moff & 0x0000ff00)
			*out++ = moff >> 8,  i |= 0x02;
		if (moff & 0x00ff0000)
			*out++ = moff >> 16, i |= 0x04;
		if (moff & 0xff000000)
			*out++ = moff >> 24, i |= 0x08;

		if (size & 0x00ff)
			*out++ = size >> 0, i |= 0x10;
		if (size & 0xff00)
			*out++ = size >> 8, i |= 0x20;
		
		*op = i;
	}
	*pout = out;
}

// Return: amount of bytes one would need to encode dc
inline
ushort DC_count_encode_bytes(const DeltaChunk* dc)
{
	if (dc->data){
		return 1 + dc->ts;		// cmd byte + actual data bytes
	} else {
		ushort c = 1;			// cmd byte
		uint ts = dc->ts;
		ull so = dc->so;
		
		// offset
		c += so & 0x000000FF;
		c += so & 0x0000FF00;
		c += so & 0x00FF0000;
		c += so & 0xFF000000;
		
		// size - max size is 0x10000, its encoded with 0 size bits
		c += ts & 0x000000FF;
		c += ts & 0x0000FF00;
		
		return c;
	}
}



// DELTA INFO
/////////////
typedef struct {
	uint dso;			// delta stream offset
	uint to;			// target offset (cache)
} DeltaInfo;


// DELTA INFO VECTOR
//////////////////////

typedef struct {
	DeltaInfo		*mem;					// Memory for delta infos
	uint			 di_last_size;			// size of the last element - we can't compute it using the next bound 
	const uchar		*dstream;				// borrowed ointer to delta stream we index
	Py_ssize_t 		size;					// Amount of DeltaInfos
	Py_ssize_t 		reserved_size;			// Reserved amount of DeltaInfos
} DeltaInfoVector;



// Reserve enough memory to hold the given amount of delta chunks
// Return 1 on success
// NOTE: added a minimum allocation to assure reallocation is not done 
// just for a single additional entry. DIVs change often, and reallocs are expensive
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
	vec->dstream = NULL;
	vec->size = 0;
	vec->reserved_size = 0;
	vec->di_last_size = 0;
	
	return DIV_grow_by(vec, initial_size);
}

inline
Py_ssize_t DIV_len(const DeltaInfoVector* vec)
{
	return vec->size;
}

inline
uint DIV_lbound(const DeltaInfoVector* vec)
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

// return rbound offset in bytes. We use information contained in the 
// vec to do that
inline
uint DIV_info_rbound(const DeltaInfoVector* vec, const DeltaInfo* di)
{
	if (DIV_last(vec) == di){
		return di->to + vec->di_last_size;
	} else {
		return (di+1)->to;
	}
}

// return size of the given delta info item
inline
uint DIV_info_size2(const DeltaInfoVector* vec, const DeltaInfo* di, const DeltaInfo const* veclast)
{
	if (veclast == di){
		return vec->di_last_size;
	} else {
		return (di+1)->to - di->to;
	}
}

// return size of the given delta info item
inline
uint DIV_info_size(const DeltaInfoVector* vec, const DeltaInfo* di)
{
	return DIV_info_size2(vec, di, DIV_last(vec));
}

void DIV_destroy(DeltaInfoVector* vec)
{
	if (vec->mem){
#ifdef DEBUG
		fprintf(stderr, "Freeing %p\n", (void*)vec->mem);
#endif
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

// Reset the vector so that its size will be zero
// It will keep its memory though, and hence can be filled again
inline
void DIV_reset(DeltaInfoVector* vec)
{
	if (vec->size == 0)
		return;
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
	DeltaInfo* di;
	
	while (lo < hi)
	{
		mid = (lo + hi) / 2;
		di = vec->mem + mid;
		if (di->to > ofs){
			hi = mid;
		} else if ((DIV_info_rbound(vec, di) > ofs) | (di->to == ofs)) {
			return di;
		} else {
			lo = mid + 1;
		}
	}
	
	return DIV_last(vec);
}

// forward declaration
const uchar* next_delta_info(const uchar*, DeltaChunk*);

// Return the amount of chunks a slice at the given spot would have, as well as 
// its size in bytes it would have if the possibly partial chunks would be encoded
// and added to the spot marked by sdc
inline
uint DIV_count_slice_bytes(const DeltaInfoVector* src, uint ofs, uint size)
{
	uint num_bytes = 0;
	DeltaInfo* cdi = DIV_closest_chunk(src, ofs);
	
	
	
	DeltaChunk dc;
	DC_init(&dc, 0, 0, 0, NULL);
	
	// partial overlap
	if (cdi->to != ofs) {
		const ull relofs = ofs - cdi->to;
		const uint cdisize = DIV_info_size(src, cdi);
		const uint actual_size = cdisize - relofs < size ? cdisize - relofs : size; 
		size -= actual_size;
		
		// get the size in bytes the info would have
		next_delta_info(src->dstream + cdi->dso, &dc);
		dc.so += relofs;
		dc.ts = actual_size;
		num_bytes += DC_count_encode_bytes(&dc);
		
		cdi += 1;
		
		if (size == 0){
			return num_bytes;
		}
	}
	
	const DeltaInfo const* vecend = DIV_end(src);
	for( ;cdi < vecend; ++cdi){
		next_delta_info(src->dstream + cdi->dso, &dc);
		
		if (dc.ts < size) {
			num_bytes += DC_count_encode_bytes(&dc);
			size -= dc.ts;
		} else {
			dc.ts = size;
			num_bytes += DC_count_encode_bytes(&dc);
			size = 0;
			break;
		}
	}
	
	assert(size == 0);
	return num_bytes;
}

// Write a slice as defined by its absolute offset in bytes and its size into the given
// destination memory. The individual chunks written will be a byte copy of the source 
// data chunk stream
// Return: number of chunks in the slice
inline
uint DIV_copy_slice_to(const DeltaInfoVector* src, uchar* dest, ull tofs, uint size)
{
	assert(DIV_lbound(src) <= tofs);
	assert((tofs + size) <= DIV_last(src)->to + src->di_last_size);
	
	DeltaChunk dc;
	DC_init(&dc, 0, 0, 0, NULL);
	
	DeltaInfo* cdi = DIV_closest_chunk(src, tofs);
	uint num_chunks = 0;
	
	// partial overlap
	if (cdi->to != tofs) {
		const uint relofs = tofs - cdi->to;
		next_delta_info(src->dstream + cdi->dso, &dc);
		const uint cdisize = dc.ts;
		const uint actual_size = cdisize - relofs < size ? cdisize - relofs : size; 
		
		size -= actual_size; 
		
		// adjust dc proportions
		
		DC_encode_to(&dc, &dest, relofs, actual_size);
			
		num_chunks += 1;
		cdi += 1;
		
		if (size == 0){
			return num_chunks;
		}
	}
	
	const DeltaInfo* vecend = DIV_end(src);
	for( ;cdi < vecend; ++cdi)
	{
		num_chunks += 1;
		next_delta_info(src->dstream + cdi->dso, &dc);
		if (dc.ts < size) {
			// Full copy would be possible, but the final length of the dstream
			// needs to be used as well to know how many bytes to copy
			// TODO: make a DIV_ function for this
			DC_encode_to(&dc, &dest, 0, dc.ts);
			size -= dc.ts;
		} else {
			DC_encode_to(&dc, &dest, 0, size);
			size = 0;
			break;
		}
	}
	
	assert(size == 0);
	return num_chunks;
}


// Take slices of div into the corresponding area of the tsi, which is the topmost
// delta to apply.
bool DIV_connect_with_base(ToplevelStreamInfo* tsi, DeltaInfoVector* div)
{
	assert(tsi->num_chunks);
	
	typedef struct {
		uint bofs;			// byte-offset of delta stream
		uint dofs;			// delta stream offset relative to tsi->cstart
	} OffsetInfo;
	
	
	OffsetInfo *const offset_array = PyMem_Malloc(tsi->num_chunks * sizeof(OffsetInfo));
	if (!offset_array){
		return 0;
	}
	
	OffsetInfo* pofs = offset_array;
	uint num_addbytes = 0;
	
	const uchar* data = TSI_first(tsi);
	const uchar* prev_data = data;
	const uchar const* dend = TSI_end(tsi);
	
	DeltaChunk dc;
	DC_init(&dc, 0, 0, 0, NULL);
	
	// OFFSET RUN
	for (;data < dend; pofs++, prev_data = data)
	{
		
		pofs->bofs = num_addbytes;
		pofs->dofs = (uint)(prev_data - data);
		
		// Data chunks don't need processing
		data = next_delta_info(data, &dc);
		
		if (dc.data){
			continue;
		}
		
		// offset the next chunk by the amount of chunks in the slice
		// - N, because we replace our own chunk's bytes
		num_addbytes += DIV_count_slice_bytes(div, dc.so, dc.ts) - (data - prev_data);
	}
	
	
	
	// reserve enough memory to hold all the new chunks
	TSI_resize(tsi, tsi->tdslen + num_addbytes);
	const OffsetInfo const* pofs_start = offset_array - 1;
	const OffsetInfo* cpofs;
	uchar* ds;											// pointer into the delta stream
	const uchar* nds;									// next pointer, used for size retrieving the size
	uint num_addchunks = 0;								// total amount of chunks added
	
	// Insert slices, from the end to the beginning, which allows memcpy
	// to be used, with a little help of the offset array
	for (cpofs = pofs - 1; cpofs > pofs_start; cpofs--)
	{
		ds = (uchar*)(tsi->cstart + cpofs->dofs);
		nds = next_delta_info(ds, &dc);
		
		// Data chunks don't need processing
		if (dc.data){
			// NOTE: could peek the preceeding chunks to figure out whether they are 
			// all just moved by ofs. In that case, they can move as a whole!
			// tests showed that this is very rare though, even in huge deltas, so its
			// not worth the extra effort
			if (pofs->bofs){
				memcpy((void*)(ds + cpofs->bofs), (void*)ds, nds - ds);
			}
			continue;
		}
		
		// Copy Chunks - target offset is determined by their location and size
		// hence it doesn't need specific adjustment
		// -1 chunks because we overwrite our own chunk ( by not copying it ) 
		num_addchunks += DIV_copy_slice_to(div, ds + cpofs->bofs, dc.so, dc.ts);
		num_addchunks -= 1;
	}
	
	tsi->num_chunks += num_addchunks;
	
	PyMem_Free(offset_array);
	return 1;

}

// DELTA CHUNK LIST (PYTHON)
/////////////////////////////
// Internally, it has nothing to do with a ChunkList anymore though
typedef struct {
	PyObject_HEAD
	// -----------
	ToplevelStreamInfo istream;
	
} DeltaChunkList;


static 
int DCL_init(DeltaChunkList*self, PyObject *args, PyObject *kwds)
{
	if(args && PySequence_Size(args) > 0){
		PyErr_SetString(PyExc_ValueError, "Too many arguments");
		return -1;
	}
	
	TSI_init(&self->istream);
	return 0;
}

static
void DCL_dealloc(DeltaChunkList* self)
{
	TSI_destroy(&(self->istream));
}

static
PyObject* DCL_py_rbound(DeltaChunkList* self)
{
	return PyLong_FromUnsignedLongLong(self->istream.target_size);
}

// Write using a write function, taking remaining bytes from a base buffer
static
PyObject* DCL_apply(DeltaChunkList* self, PyObject* args)
{
	/*
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
	*/
	// TODO
	assert(0);
	Py_RETURN_NONE;
}

static PyMethodDef DCL_methods[] = {
    {"apply", (PyCFunction)DCL_apply, METH_VARARGS, "Apply the given iterable of delta streams" },
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
	return dcl;
}

// Read the next delta chunk from the given stream and advance it
// dc will contain the parsed information, its offset must be set by
// the previous call of next_delta_info, which implies it should remain the 
// same instance between the calls.
// Return the altered uchar pointer, reassign it to the input data
inline
const uchar* next_delta_info(const uchar* data, DeltaChunk* dc)
{
	const char cmd = *data++;

	if (cmd & 0x80) 
	{
		uint cp_off = 0, cp_size = 0;
		if (cmd & 0x01) cp_off = *data++;
		if (cmd & 0x02) cp_off |= (*data++ << 8);
		if (cmd & 0x04) cp_off |= (*data++ << 16);
		if (cmd & 0x08) cp_off |= ((unsigned) *data++ << 24);
		if (cmd & 0x10) cp_size = *data++;
		if (cmd & 0x20) cp_size |= (*data++ << 8);
		if (cmd & 0x40) cp_size |= (*data++ << 16);		// this should never get hit with current deltas ...
		if (cp_size == 0) cp_size = 0x10000;
	
		dc->to += dc->ts;
		dc->data = NULL;
		dc->so = cp_off;
		dc->ts = cp_size;
		
	} else if (cmd) {
		// Just share the data
		dc->to += dc->ts;
		dc->data = data;
		dc->ts = cmd;
		dc->so = 0;
	} else {                                                                               
		PyErr_SetString(PyExc_RuntimeError, "Encountered an unsupported delta cmd: 0");
		return NULL;
	}
	
	return data;
}

// Return amount of chunks encoded in the given delta stream
// If read_header is True, then the header msb chunks will be read first.
// Otherwise, the stream is assumed to be scrubbed one past the header
uint compute_chunk_count(const uchar* data, const uchar* dend, bool read_header)
{
	// read header
	if (read_header){
		msb_size(&data, dend);
		msb_size(&data, dend);
	}
	
	DeltaChunk dc;
	DC_init(&dc, 0, 0, 0, NULL);
	uint num_chunks = 0;
	
	while (data < dend)
	{
		data = next_delta_info(data, &dc);
		num_chunks += 1;
	}// END handle command opcodes
	
	return num_chunks;
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
	
	DeltaInfoVector div;
	ToplevelStreamInfo tdsinfo;
	TSI_init(&tdsinfo);
	DIV_init(&div, 100);			// should be enough to keep the average text file
	
	
	// GET TOPLEVEL DELTA STREAM
	int error = 0;
	PyObject* ds = 0;
	unsigned int dsi = 0;			// delta stream index we process
	ds = PyIter_Next(stream_iter);
	if (!ds){
		error = 1;
		goto _error;
	}
	
	dsi += 1;
	tdsinfo.parent_object = PyObject_CallMethod(ds, "read", 0);
	if (!PyObject_CheckReadBuffer(tdsinfo.parent_object)){
		Py_DECREF(ds);
		error = 1;
		goto _error;
	}
	
	PyObject_AsReadBuffer(tdsinfo.parent_object, (const void**)&tdsinfo.tds, &tdsinfo.tdslen);
	if (tdsinfo.tdslen > pow(2, 32)){
		// parent object is deallocated by info structure
		Py_DECREF(ds);
		PyErr_SetString(PyExc_RuntimeError, "Cannot handle deltas larger than 4GB");
		tdsinfo.parent_object = 0;
		
		error = 1;
		goto _error;
	}
	Py_DECREF(ds);
	
	// let it officially know, and initialize its internal state
	TSI_set_stream(&tdsinfo, tdsinfo.tds);
	
		// INTEGRATE ANCESTOR DELTA STREAMS
	for (ds = PyIter_Next(stream_iter); ds != NULL; ds = PyIter_Next(stream_iter), ++dsi)
	{
		// Its important to initialize this before the next block which can jump 
		// to code who needs this to exist !
		PyObject* db = 0;
		
		// When processing the first delta, we know we will have to alter the tds
		// Hence we copy it and deallocate the parent object
		if (dsi == 1) {
			if (!TSI_copy_stream_from_object(&tdsinfo)){
				PyErr_SetString(PyExc_RuntimeError, "Could not allocate memory to copy toplevel buffer");
				// info structure takes care of the parent_object
				error = 1;
				goto loop_end;
			}
			
			tdsinfo.num_chunks = compute_chunk_count(tdsinfo.cstart, TSI_end(&tdsinfo), 0);
		}
		
		db = PyObject_CallMethod(ds, "read", 0);
		if (!PyObject_CheckReadBuffer(db)){
			error = 1;
			PyErr_SetString(PyExc_RuntimeError, "Returned buffer didn't support the buffer protocol");
			goto loop_end;
		}
		
		// Fill the stream info structure
		const uchar* data;
		Py_ssize_t dlen;
		PyObject_AsReadBuffer(db, (const void**)&data, &dlen);
		const uchar const* dstart = data;
		const uchar const* dend = data + dlen;
		div.dstream = dstart;
		
		if (dlen > pow(2, 32)){
			error = 1;
			PyErr_SetString(PyExc_RuntimeError, "Cannot currently handle deltas larger than 4GB");
			goto loop_end;
		}
		
		// READ HEADER
		msb_size(&data, dend);
		const ull target_size = msb_size(&data, dend);
		
		DIV_reserve_memory(&div, compute_chunk_count(data, dend, 0));
	
		// parse command stream
		DeltaChunk dc;
		DeltaInfo* di = 0;				// temporary pointer
		DC_init(&dc, 0, 0, 0, NULL);
		
		assert(data < dend);
		while (data < dend)
		{
			di = DIV_append(&div);
			di->dso = data - dstart;
			if ((data = next_delta_info(data, &dc))){
				di->to = dc.to;
			} else { 
				error = 1;
				goto loop_end;
			}
		}// END handle command opcodes
		
		// finalize information
		div.di_last_size = dc.ts;
		
		if (DC_rbound(&dc) != target_size){
			PyErr_SetString(PyExc_RuntimeError, "Failed to parse delta stream");
			error = 1;
		}
		
		if (!DIV_connect_with_base(&tdsinfo, &div)){
			error = 1;
		}
	
		#ifdef DEBUG
		fprintf(stderr, "tdsinfo->len = %i\n", (int)tdsinfo.tdslen);
		fprintf(stderr, "div->size = %i, div->reserved_size = %i\n", (int)div.size, (int)div.reserved_size);
		#endif
		
		// destroy members, but keep memory
		DIV_reset(&div);

loop_end:
		// perform cleanup
		Py_DECREF(ds);
		Py_DECREF(db);
		
		if (error){
			break;
		}
	}// END for each stream object
	
	if (dsi == 0){
		PyErr_SetString(PyExc_ValueError, "No streams provided");
	}
	
	
_error:
	
	if (stream_iter != dstreams){
		Py_DECREF(stream_iter);
	}
	
	
	DIV_destroy(&div);
	
	// Return the actual python object - its just a container
	DeltaChunkList* dcl = DCL_new_instance();
	if (!dcl){
		PyErr_SetString(PyExc_RuntimeError, "Couldn't allocate list");
		// Otherwise tdsinfo would be deallocated by the chunk list
		TSI_destroy(&tdsinfo);
		error = 1;
	} else {
		// Plain copy, transfer ownership to dcl
		dcl->istream = tdsinfo;
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
