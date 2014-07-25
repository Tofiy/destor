#include <mysql.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <stdint.h>
#include "kvstore_mysql.h"

unsigned int RSHash  (unsigned char *, unsigned int);
unsigned int DJBHash (unsigned char *, unsigned int);
unsigned int FNVHash (unsigned char *, unsigned int);
unsigned int JSHash  (unsigned char *, unsigned int);
unsigned int PJWHash (unsigned char *, unsigned int);
unsigned int SDBMHash(unsigned char *, unsigned int);
unsigned int DEKHash (unsigned char *, unsigned int);

void insert_word(unsigned char[], char *, int);
int in_dict(unsigned char[], char *, int);
void clean_word(unsigned char[], char *, int );

static MYSQL mysql;
unsigned char *filter;
int64_t *values;

void init_kvstore_mysql(){
	mysql_init(&mysql);
	filter=(char *)calloc((SIZE+CHAR_BIT-1)/CHAR_BIT,sizeof(char));
	 if (filter == NULL)  
	  {  
	  exit(0);  
	  } 
      	if (!mysql_real_connect(&mysql,"localhost", "root", "123689", "kvstore_db",0,NULL,0))
	  printf( "Error connecting to database: %s\n",mysql_error(&mysql));
	else{
	  int i;
	  for(i=0;i<(SIZE+CHAR_BIT-1)/CHAR_BIT;i++) {  
        		filter[i]=0;  
   		  } 
	FILE *fp;
	  if ((fp = fopen("bf.txt", "rb")) == NULL)  
	  {  
	  exit(0);  
	  } 
	  fseek(fp, 0, SEEK_SET);  
	  fread(filter, (SIZE+CHAR_BIT-1)/CHAR_BIT, sizeof(char), fp);  
	  fclose(fp); 
	   } 
}	 

void close_kvstore_mysql(){
	mysql_close(&mysql);
	FILE *fp;
	  if ((fp = fopen("bf.txt", "wb")) == NULL)  
	  {  
    	  exit(0);  
	  }  
	rewind(fp);  
	fwrite(filter, (SIZE+CHAR_BIT-1)/CHAR_BIT, sizeof(char), fp);  
	fclose(fp);
	free(filter);
	free(values);
}

void kvstore_mysql_update(char* theKey,int64_t theValue){
	char query[256];
	int t;
	MYSQL_RES *res;
      	MYSQL_ROW row;
	
	sprintf(query,"INSERT INTO Newhash VALUES(NULL,'%s',%lld)",theKey,theValue);
        t = mysql_real_query(&mysql,query,(unsigned int) strlen(query));
 	if (t)
	printf("Error making query: %s\n",mysql_error(&mysql));
	else insert_word(filter,theKey,strlen(theKey));

	sprintf(query,"SELECT COUNT(theKey) FROM Newhash WHERE theKey='%s'",theKey);
	t = mysql_real_query(&mysql,query,(unsigned int) strlen(query));
 	if (t)
	printf("Error making query: %s\n",mysql_error(&mysql));
	res = mysql_store_result(&mysql); 
 	row = mysql_fetch_row(res);	
	   if(atoi(row[0])>MAX_VALUE_NUM){
		sprintf(query,"SELECT id FROM Newhash WHERE theKey='%s' ORDER BY id ASC",theKey);
		t = mysql_real_query(&mysql,query,(unsigned int) strlen(query));
 		if (t)
		printf("Error making query: %s\n",mysql_error(&mysql));
		res = mysql_store_result(&mysql); 
 		row = mysql_fetch_row(res);
		
		sprintf(query,"DELETE FROM Newhash WHERE id='%s'",row[0]);
		t = mysql_real_query(&mysql,query,(unsigned int) strlen(query));
 		if (t)
		printf("Error making query: %s\n",mysql_error(&mysql));	
		}
	mysql_free_result(res);
}

int64_t* kvstore_mysql_lookup(char* theKey){
	char query[256];
	int t,i=0;
	values=(int64_t *)calloc(MAX_VALUE_NUM+1,sizeof(int64_t));
	MYSQL_RES *res;
      	MYSQL_ROW row;
	if(in_dict(filter,theKey,strlen(theKey))){
	sprintf(query,"SELECT theValue FROM Newhash WHERE theKey='%s'",theKey);
	t = mysql_real_query(&mysql,query,(unsigned int) strlen(query));
 	  if (t)
   		printf("Error making query: %s\n",mysql_error(&mysql));
	  else {
		res = mysql_store_result(&mysql); 
 		while(row = mysql_fetch_row(res))
 		 {
 			values[i]=atoi(row[0]);
			i++;
 		  }
		values[i]=TEMPORARY_ID;
		
		}
		mysql_free_result(res);
		return values;
            }
		else return NULL;
 		
}

void kvstore_mysql_delete(char* theKey,int64_t theValue){
	char query[256];
	int t;
	MYSQL_RES *res;
      	MYSQL_ROW row;
	sprintf(query,"DELETE FROM Newhash WHERE theKey='%s' and theValue=%lld",theKey,theValue);
	t = mysql_real_query(&mysql,query,(unsigned int) strlen(query));
 	  if (t)
    	  printf("Error making query: %s\n",mysql_error(&mysql));
		
	sprintf(query,"SELECT COUNT(theKey) FROM Newhash WHERE theKey='%s'",theKey);
	t = mysql_real_query(&mysql,query,(unsigned int) strlen(query));
 	  if (t)
	  printf("Error making query: %s\n",mysql_error(&mysql));
	res = mysql_store_result(&mysql); 
 	row = mysql_fetch_row(res);	
	  if(atoi(row[0])==0) 
	  clean_word(filter,theKey,strlen(theKey));
	mysql_free_result(res);
} 


unsigned int (*hash_func[])(unsigned char *, unsigned int) = { 
    RSHash, 
    DJBHash, 
    FNVHash, 
    JSHash,
    PJWHash, 
    SDBMHash, 
    DEKHash
};

void insert_word(unsigned char filter[], char *str, int len)
{
	unsigned long hash[NUM_HASHES];
	int i;

	for (i = 0; i < NUM_HASHES; i++) {
        hash[i] = hash_func[i](str, len);
	SETBIT(filter,hash[i]%SIZE);
	}
}

int in_dict(unsigned char filter[], char *str, int len)
{
	unsigned int hash[NUM_HASHES];
	int i;

	for (i = 0; i < NUM_HASHES; i++) {
        hash[i] = hash_func[i](str, len);
	if(!GETBIT(filter,hash[i]%SIZE))
			return 0;
	}

	return 1;
}

void clean_word(unsigned char filter[], char *str, int len)
{
	unsigned long hash[NUM_HASHES];
	int i;

	for (i = 0; i < NUM_HASHES; i++) {
        hash[i] = hash_func[i](str, len);
	CLEANBIT(filter,hash[i]%SIZE);
	}
}

/****************\
| Hash Functions |
\****************/

unsigned int RSHash(unsigned char *str, unsigned int len)
{
   unsigned int b    = 378551;
   unsigned int a    = 63689;
   unsigned int hash = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = hash * a + (*str);
      a    = a * b;
   }

   return hash;
}

unsigned int JSHash(unsigned char *str, unsigned int len)
{
   unsigned int hash = 1315423911;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash ^= ((hash << 5) + (*str) + (hash >> 2));
   }

   return hash;
}

unsigned int PJWHash(unsigned char *str, unsigned int len)
{
   const unsigned int BitsInUnsignedInt = (unsigned int)(sizeof(unsigned int) * 8);
   const unsigned int ThreeQuarters     = (unsigned int)((BitsInUnsignedInt  * 3) / 4);
   const unsigned int OneEighth         = (unsigned int)(BitsInUnsignedInt / 8);
   const unsigned int HighBits          = (unsigned int)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);
   unsigned int hash              = 0;
   unsigned int test              = 0;
   unsigned int i                 = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (hash << OneEighth) + (*str);

      if((test = hash & HighBits)  != 0)
      {
         hash = (( hash ^ (test >> ThreeQuarters)) & (~HighBits));
      }
   }

   return hash;
}

unsigned int SDBMHash(unsigned char *str, unsigned int len)
{
   unsigned int hash = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (*str) + (hash << 6) + (hash << 16) - hash;
   }

   return hash;
}

unsigned int DJBHash(unsigned char *str, unsigned int len)
{
   unsigned int hash = 5381;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = ((hash << 5) + hash) + (*str);
   }

   return hash;
}

unsigned int DEKHash(unsigned char *str, unsigned int len)
{
   unsigned int hash = len;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = ((hash << 5) ^ (hash >> 27)) ^ (*str);
   }
   return hash;
}

unsigned int FNVHash(unsigned char *str, unsigned int len)
{
   const unsigned int fnv_prime = 0x811C9DC5;
   unsigned int hash      = 0;
   unsigned int i         = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash *= fnv_prime;
      hash ^= (*str);
   }

   return hash;
}
