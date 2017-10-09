main:main.o db.o table.o log.o bloom_fliter.o
	gcc -o main main.o db.o table.o log.o bloom_fliter.o -lcrypto -lssl -lpthread

main.o:db.h table.h 
	gcc -c -g -std=gnu11 main.c 

db.o:db.h table.h
	gcc -c -g -std=gnu11 db.c

bloom_fliter.o:bloom_fliter.h
	gcc -c -g -std=gnu11 bloom_fliter.c
	
table.o:table.h
	gcc -c -g -std=gnu11 table.c

log.o:log.h db.h
	gcc -c -g -std=gnu11 log.c

.PHONY: clean

clean:
	rm -f *.o log.txt 
