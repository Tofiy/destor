#ifndef KVSTORE_MYSQL_H_
#define KVSTORE_MYSQL_H_

#define MAX_VALUE_NUM 4
#define NUM_HASHES 4
#define SETBIT(a, n) (a[n/CHAR_BIT] |= (1<<(n%CHAR_BIT)))
#define GETBIT(a, n) (a[n/CHAR_BIT] & (1<<(n%CHAR_BIT)))
#define CLEANBIT(a, n) (a[n/CHAR_BIT] &= ~(1<<(n%CHAR_BIT)))
#define SIZE 1048576

void init_kvstore_mysql_();
void close_kvstore_mysql_();
void kvstore_mysql_update(char* theKey,int64_t theValue);
int64_t* kvstore_mysql_lookup(char* theKey);
void kvstore_mysql_delete(char* theKey,int64_t theValue);

#endif
