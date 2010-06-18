
_fun.o: _fun.c
	gcc -pthread -fno-strict-aliasing -DNDEBUG -g -fwrapv -O2 -Wall -Wstrict-prototypes -fPIC -I/usr/include/python2.6 -c $< -o $@

_fun.so: _fun.o
	gcc -pthread -shared -Wl,-O1 -Wl,-Bsymbolic-functions $^ -o $@
	
all: _fun.so

clean:
	-rm *.so
	-rm *.o
