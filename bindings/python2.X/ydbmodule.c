#include <Python.h>
#include <ydb.h>

#include <Python.h>

typedef struct {
	PyObject_HEAD
	YDB ydb;
} ydb_YdbObject;


static PyTypeObject ydb_YdbType;


static PyObject *YdbError;

static PyObject *
newydbobject(char *file, int overcommit_factor)
{
	ydb_YdbObject *yo;

	yo = PyObject_New(ydb_YdbObject, &ydb_YdbType);
	if (yo == NULL)
		return NULL;
	yo->ydb = ydb_open(file, overcommit_factor, 5*1024*1024);
	if(yo->ydb == NULL){
		PyErr_SetFromErrno(YdbError);
		Py_DECREF(yo);
		return NULL;
	}
	return (PyObject *)yo;
}

static PyObject *
ydb__close(register ydb_YdbObject *yo, PyObject *unused) {
	if (yo->ydb)
		ydb_close(yo->ydb);
	yo->ydb = NULL;
	Py_INCREF(Py_None);
	return Py_None;
}



static PyMethodDef ydb_methods[] = {
	{"close",	(PyCFunction)ydb__close,	METH_NOARGS,
	 "close()\nClose the database."},
/*	{"get",		(PyCFunction)dbm_get,		METH_VARARGS,
	 "get(key[, default]) -> value\n"
	 "Return the value for key if present, otherwise default."},*/
	{NULL,		NULL}		/* sentinel */
};

static PyObject *
ydb_getattr(ydb_YdbObject *yo, char *name) {
	return Py_FindMethod(ydb_methods, (PyObject *)yo, name);
}

static void
ydb_dealloc(register ydb_YdbObject *yo)
{
        if ( yo->ydb )
		ydb_close(yo->ydb);
	yo->ydb = NULL;
	PyObject_Del(yo);
}


static PyTypeObject ydb_YdbType = {
	PyObject_HEAD_INIT(NULL)
	0,				/*ob_size*/
	"ydb.ydb",			/*tp_name*/
	sizeof(ydb_YdbObject),		/*tp_basicsize*/
	0,                         /*tp_itemsize*/
	(destructor)ydb_dealloc,   /*tp_dealloc*/
	0,			   /*tp_print*/
	(getattrfunc)ydb_getattr,  /*tp_getattr*/
	0,                         /*tp_setattr*/
	0,                         /*tp_compare*/
	0,                         /*tp_repr*/
	0,                         /*tp_as_number*/
	0,                         /*tp_as_sequence*/
	0,                         /*tp_as_mapping*/
	0,                         /*tp_hash */
	0,                         /*tp_call*/
	0,                         /*tp_str*/
	0,                         /*tp_getattro*/
	0,                         /*tp_setattro*/
	0,                         /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT,        /*tp_flags*/
	"ydb.ydb object",		/* tp_doc */
};

static PyObject *
ydbopen(PyObject *self, PyObject *args) {
	char *name;
	int overcommit_factor = 5;

        if ( !PyArg_ParseTuple(args, "s|i:open", &name, &overcommit_factor) )
		return NULL;
	if(overcommit_factor < 2 || overcommit_factor > 999) {
		PyErr_SetString(YdbError,
				"arg 2 to open integer between range 1...999");
		return NULL;
	}
        return newydbobject(name, overcommit_factor);
}


static PyMethodDef ydbmodule_methods[] = {
	{ "open", (PyCFunction)ydbopen, METH_VARARGS,
	  "open(path[, overcommit_factor]) -> mapping\n"
	  "Return a YDB database object."},
	{ 0, 0 },
};


PyMODINIT_FUNC
initydb(void) {
	PyObject* m;

	ydb_YdbType.tp_new = PyType_GenericNew;
	if (PyType_Ready(&ydb_YdbType) < 0)
		return;

	m = Py_InitModule("ydb", ydbmodule_methods);
}

/*
//static PyObject *SpamError;

static PyObject *qlist_arrayI_tostring(PyObject *self, PyObject *args)
{
	char *arr;
	int arr_sz;
	if (!PyArg_ParseTuple(args, "s#", &arr, &arr_sz))
		return NULL;
	if (arr_sz < 0)
		return NULL;
	u_int32_t *values = (u_int32_t *)arr;
	int values_sz = arr_sz/4;
	u_int32_t *values_end = values + values_sz;
	
	char v[MAX_WIRE_VALUE_SIZE];
	QLIST *qo = (QLIST*)v;
	qlist_initialize(qo, 0, MAX_WIRE_VALUE_SIZE);
	QITER_INIT(qo);
	
	for(;values < values_end; values++) {
		QITER_PUT(qo, *values);
	}
	qlist_freeze(qo);
	
	return PyString_FromStringAndSize(v, sizeof(QLIST)+qo->list_sz);
}

static PyMethodDef QListMethods[] =
{
	{"tostring", qlist_arrayI_tostring, METH_VARARGS},
	{NULL, NULL}
};

void init_qlist() {
	PyObject *m, *d;
	
	m = Py_InitModule("_qlist", QListMethods);
	d = PyModule_GetDict(m);
	//SpamError = PyErr_NewException("spam.error", NULL, NULL);
	//PyDict_SetItemString(d, "error", SpamError);
}

*/
