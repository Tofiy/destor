#ifndef KVSTORE_MYSQL_H_
#define KVSTORE_MYSQL_H_

#define MAX_VALUE_NUM 4
#define TEMPORARY_ID (-1)l

void init_kvstore_mysql_();
void close_kvstore_mysql_();
void kvstore_mysql_update(char* theKey,int64_t theValue);
int64_t* kvstore_mysql_lookup(char* theKey);
void kvstore_mysql_delete(char* theKey,int64_t theValue);

#endif
