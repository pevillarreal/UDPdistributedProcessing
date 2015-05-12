/*	
	
	udp master node,reads settings from file, 
	reads keys from file, sends them to however many nodes set
	and waits for sorted keys and then the statistical packet
	--------DATA FILE structure---------
	<hostname> <port>

	---------KEYS------------
	<key> --- on in each line


*/
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#define CONF_FILE	"data.txt"	/*contains node hostname and port, rename as needed */
#define OUT_FILE	"out.txt"
#define num_of_procs	1		/*number of processors to send data to*/
#define MAX				10		/*max number of keys */
#define min_num		350		/*starting number */
#define max_num		51000
#define PORT	4001
#define ACK	"ACK";
#define KEYS_FILE "keys.txt"

 
typedef struct
{
	int nums[MAX];
	char hostname[100];
	int numofkeys;
	int numofkeysrecvd ;
	int	port_no;
	time_t timeused;
	int minrange;
	int maxrange;
	char msg[10];
	int done;
} Bucket;

typedef struct
{
	int numOfKeys;
	time_t sorttime;
} KeyData;

int main()
{
	Bucket buckets[num_of_procs];
	int sd;
	int sd2;
	FILE *inFP;
	char hostname[100];
	int portnum;
	int i;
	time_t totaltime;
	int n = min_num;
	struct hostent *hp;
	struct sockaddr_in sin;
	struct sockaddr_in pin;
	char msg[10];
	time_t waittime;
	int recv;


	for ( i = 0;i<num_of_procs;i++) /* initialize data */
	{
		buckets[i].numofkeys = 0;
		buckets[i].numofkeysrecvd =0;
		buckets[i].done = 0;
		memset(buckets[i].hostname,0,sizeof(buckets[i].hostname));
		buckets[i].minrange = n;
		n = ((max_num-min_num)/num_of_procs*(i+1))+min_num;
		if ( i == num_of_procs -1 )
			buckets[i].maxrange = n+1;
		else
			buckets[i].maxrange = n;
	}
	
	/* read in and set data for each node */
	inFP = fopen(CONF_FILE,"r");
	printf("Reading %s for configuration details.\n",CONF_FILE);
	for ( i = 0;i < num_of_procs; i++)
	{
		fscanf(inFP,"%s %d", hostname, &portnum);
		strcpy(buckets[i].hostname,hostname);
		buckets[i].port_no = portnum;

	}
	fclose(inFP);


	/****** read numbers ******/
	FILE *keysFP;
	int keynum;
	keysFP = fopen(KEYS_FILE,"r");
	while ( fscanf(keysFP,"%d",&keynum) != EOF)
	{
		 /* separate numbers into its buckets */
		for ( i = 0; i<= num_of_procs;i++)
		{
			if ( buckets[i].minrange <= keynum && keynum < buckets[i].maxrange )
			{
				buckets[i].nums[buckets[i].numofkeys]= keynum;
				buckets[i].numofkeys++;
			}
		}
	}
	fclose(keysFP);
	int j;
	int tempint;
	if (( sd2 = socket(AF_INET, SOCK_DGRAM,0)) == -1 ) //send socket
	{
		perror("socket");
		exit(1);
	}
	if (( sd = socket(AF_INET, SOCK_DGRAM,0)) == -1 ) //receive socket
	{
		perror("socket");
		exit(1);
	}

	memset(&pin, 0, sizeof(pin));
	pin.sin_family = AF_INET;

	//* complete the socket structure 
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_ANY);
	sin.sin_port = htons(PORT);
	int structlength = sizeof(sin);
	//* bind the socket to the port number
	if (bind(sd, (struct sockaddr *) &sin, sizeof(sin)) == -1) //receive socket
	{
		perror("bind");
		exit(1);
	}


	for ( i = 0;i < num_of_procs; i++)
	{
		if ((hp = gethostbyname(buckets[i].hostname)) == 0) 
		{
			perror("gethostbyname");
			exit(1);
		}

		pin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;
		pin.sin_port = htons(buckets[i].port_no);
		for( j = 0; j < buckets[i].numofkeys; j++)  //* send data in each bucket to perspective node
		{	tempint = buckets[i].nums[j];
			if (sendto(sd2, &tempint, sizeof(tempint), 0, (struct sockaddr *) &pin,sizeof(pin)) < 0) 
			{
		             perror("sendto send loop");
			}
			printf("sending %d to node %d\n",tempint,i);
			
		}
		printf("sending -1\n");
		tempint = -1;
		if (sendto(sd2, &tempint, sizeof(tempint), 0, (struct sockaddr *) &pin,sizeof(pin)) < 0) 
		{
	             perror("sendto send loop -1");
		}
		sleep(1);

	/*	char tempchar[10] = ACK;

		time_t sendTime = time(NULL) + 2;
		while ( strcmp(msg,tempchar) != 0 || time(NULL) > sendTime )
		{
			tempint = -1;
			if (sendto(sd2, &tempint, sizeof(tempint), 0, (struct sockaddr *) &pin,sizeof(pin)) < 0) 
			{
		             perror("sendto send loop -1");
			}
			/waittime = time(NULL) + 5;
			memset(msg,0,sizeof(msg));
			while(time(NULL) <= waittime )
			{
				recv = recvfrom(sd, msg, sizeof(msg), 0,(struct sockaddr *) &sin, &structlength);
				if (recv < 0) 
				{
					perror("recvfrom");
					exit(1);
				}
				if (recv > 0) 
				{
					printf("%05d: %d\n", ++i, msg);
			            memset(msg, 0, sizeof(msg));
				}
			}
			printf("msg: %s\n",msg); 
		}  */
	}


	j = 0;
	int j2 = 0;
	int procsWorking = 1;
	printf("waiting for sorted data\n");
	time_t procWaitTime = time(NULL) + 300;
	while(procsWorking)
	{

		recv = recvfrom(sd, &tempint, sizeof(tempint), 0,(struct sockaddr *) &sin, &structlength);

		if ( recv < 0 )
		{
			perror("recvfrom");
			exit(1);
		}

		if ( recv > 0 )
		{	printf("got key: %d\n",tempint);
			i = 0;
			while ( sin.sin_addr.s_addr != pin.sin_addr.s_addr )
			{
				if ((hp = gethostbyname(buckets[i].hostname)) == 0) 
				{
					perror("gethostbyname");
					exit(1);
				}
				pin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;
				pin.sin_port = htons(buckets[i].port_no);
				++i;
			}
			
			if ( tempint == -1 ) /* if it is -1 wait for packet statistical packet */
			{
				
	
				int donerec = 1;
				KeyData stats;
				while(donerec) /* keep looping until it receives statistical packet */
				{
					recv = recvfrom(sd, &stats, sizeof(KeyData), 0, (struct sockaddr*) &sin, &structlength);
					if ( recv  < 0 )
					{
						perror("recvdstat");
						exit(1);
					}
					if ( recv > 0  )
					{	/* if received data is from node that is to send statistical packet process it */
						if ( sin.sin_addr.s_addr == pin.sin_addr.s_addr )
						{
							printf("received statistical packet.\n");
							char tempchar[10] = ACK;
							strcpy(msg,tempchar);
							tempint = 1;
							time_t tempTime = time(NULL)+5;
							while( tempint != -1 && time(NULL) < tempTime)
							{
							  if (sendto(sd2, &msg, sizeof(msg), 0, (struct sockaddr *) &pin, sizeof(pin)) < 0) 
							  {
							  	perror("sendto");
							  }
							  recv = recvfrom(sd, &tempint, sizeof(tempint), 0, (struct sockaddr*) &sin, &structlength);
							  if ( sin.sin_addr.s_addr =! pin.sin_addr.s_addr )
								tempint == 1;
							}
							buckets[i].numofkeysrecvd = stats.numOfKeys;
							buckets[i].timeused = stats.sorttime;
							donerec = 0;
							buckets[i].done = 1;
							
						}
					}
				}
			}
			else /* otherise just insert key in correct spot */
			{
				buckets[i].nums[buckets[i].numofkeysrecvd ] = tempint;
				buckets[i].numofkeysrecvd++;
			}
		}
		tempint = 0;
		for( i = 0; i < num_of_procs; i++)
		{
			if ( buckets[i].done )
				tempint++;
		}
		if ( tempint == num_of_procs )
			 procsWorking = 0;
	}
	FILE *outFP;
	outFP = fopen(OUT_FILE,"w+");	
	printf("PROC_ID		SORT_TIME	KEYS_RECEIVED	KEYS_SENT\n");
	for ( i = 0; i<num_of_procs; i++) 
	{
		printf("  %d		%d		%d		%d\n",j,(int)buckets[i].timeused,buckets[i].numofkeysrecvd,buckets[i].numofkeys);
	}
	printf("sorted keys: \n");
	for( i = 0; i<num_of_procs;i++)
	{
		for(j = 0; j < buckets[i].numofkeysrecvd;j++)
		{
			printf("%d ",buckets[i].nums[j]);
			fprintf(outFP,"\n%d \n",buckets[i].nums[j]);
		}
	}
	
	fclose(outFP);
	close(sd);
	sleep(1);
	return 0;
}
