#include <mysql.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <stdint.h>
#include "kvstore_mysql.h"
#include "../utils/bloom_filter.h"

static MYSQL mysql;
unsigned char *filter;
int64_t *values;

void init_kvstore_mysql(){
	mysql_init(&mysql);
	filter=(char *)calloc(FILTER_SIZE_BYTES,sizeof(char));
	 if (filter == NULL)  
	  {  
	  exit(0);  
	  } 
      	if (!mysql_real_connect(&mysql,"localhost", "root", "123689", "kvstore_db",0,NULL,0))
	  printf( "Error connecting to database: %s\n",mysql_error(&mysql));
	else{
	  int i;
	  for(i=0;i<FILTER_SIZE_BYTES;i++) {  
        		filter[i]=0;  
   		  } 
	FILE *fp;
	  if ((fp = fopen("bf.txt", "rb")) == NULL)  
	  {  
	  exit(0);  
	  } 
	  fseek(fp, 0, SEEK_SET);  
	  fread(filter, FILTER_SIZE_BYTES, sizeof(char), fp);  
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
	fwrite(filter, FILTER_SIZE_BYTES, sizeof(char), fp);  
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
	
	sprintf(query,"DELETE FROM Newhash WHERE theKey='%s' and theValue=%lld",theKey,theValue);
	t = mysql_real_query(&mysql,query,(unsigned int) strlen(query));
 	  if (t)
    	  printf("Error making query: %s\n",mysql_error(&mysql));
		
} 
