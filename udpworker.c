/*	
	
	udp worker, receives keys, sorts them, gets total sort time
	sends sorted keys back to master, sends statistical packet
	--------DATA FILE structure---------
	<hostname> <port>
	

*/
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#define CONF_FILE	"data2.txt"	/*contains node hostname and port, rename as needed */
#define MAX				10		/*max number of keys */
#define PORT	4001
#define ACK "ACK"

typedef struct
{
	int numOfKeys;
	time_t sorttime;
} KeyData;

void swap (int *, int*);
void sort(int [], int , int );
int main ()
{
	int sd,sd2, portnum;
	int addrlen;
	struct sockaddr_in sin;
	struct sockaddr_in pin;
	int recvd;
	int key;
	int keycounter = 0;
	int keys[MAX];
	FILE *inFP;
	char hostname[100];
	struct hostent *hp;
	char msg[10];
	int tempint;
	/* get socket */
	if ((sd = socket(AF_INET, SOCK_DGRAM, 0)) == -1 )
	{
		perror("socket");
		exit(1);
	}
	printf("after sockeet\n");
	memset(&sin, 0,sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_ANY);
	sin.sin_port = htons(PORT);
	/* bind sockeet to listen */
	if (bind(sd, (struct sockaddr *) &sin, sizeof(sin)) == -1)
	{
		perror("bind");
		exit(1);
	}
	printf("after bind\n");

	/* read host data file */
	inFP = fopen("data2.txt","r");
	fscanf(inFP,"%s %d",hostname, &portnum);
	if( (hp = gethostbyname(hostname)) == 0 )
	{
		perror("gethostname");
		exit(1);
	}
	memset(&pin,0,sizeof(pin));
	pin.sin_family = AF_INET;
	pin.sin_addr.s_addr = ((struct in_addr *) (hp->h_addr))->s_addr;
	pin.sin_port = htons(PORT);
	if ((sd2 = socket(AF_INET, SOCK_DGRAM, 0)) == -1 )
	{
		perror("socket");
		exit(1);
	}

	int structlength = sizeof(sin);
	int i = 0;
	int donerec = 1;
	printf("waiting for keys\n");
	while(donerec) /* listen for keys until last key is sent (-1 ) */
	{
		recvd = recvfrom(sd, &key, sizeof(key), 0, (struct sockaddr *) &sin, &structlength);
		if ( recvd < 0 )
		{
			perror("recvrom");
			exit(1);
		}
		if ( recvd > 0 )
		{
			if ( key == -1 )
			{
				donerec = 0;
				/*char tempchar[10] = ACK;
				if (sendto(sd, tempchar, sizeof(tempchar), 0, (struct sockaddr *) &pin, sizeof(pin)) < 0) 
				{
					perror("sendto");
				} */ 
			}
			else
			{
				printf("received %d\n",key);
				keys[keycounter] = key;
				++keycounter;
			}
		}
	}
	/* sort and calculate total sort time */
	printf("sorting now\n");
	time_t startT= time(NULL);
	sort(keys,0,keycounter);
	time_t endT = time(NULL);
	KeyData stats;
	stats.numOfKeys = keycounter;
	stats.sorttime = endT - startT;
	
	printf("sending data back\n");
	for( i = 0; i < keycounter;i++) /* send sorted keys */
	{
		tempint = keys[i];
		printf("sending %d\n",tempint);
		if ( sendto(sd2,&tempint,sizeof(tempint),0,(struct sockaddr *) &pin, sizeof(pin)) < 0 )
		{
			perror("sendto key");
			exit(1);
		}
	}
	tempint = -1; /* tell master to wait for stats packet */
	if ( sendto(sd2,&tempint,sizeof(tempint),0,(struct sockaddr *) &pin, sizeof(pin)) < 0 )
	{
		perror("sent last");
		exit(1);
	}
	
	int j2 = 0;
	time_t waitTime = time(NULL) + 10;
	time_t tempTime;
	int noACK = 1;
	sleep(1);
	while ( noACK ) /* keep sending statistical packet until ACK received */
	{
		if ( sendto(sd2, &stats,sizeof(stats),0,(struct sockaddr *) &pin, sizeof(pin)) < 0 )
		{
			perror("sendto");
			exit(1);
		}
		tempTime = time(NULL) + 5;
		tempint = 0;
		while ( tempint != -1 && tempTime > time(NULL))
		{
			recvd = recvfrom(sd,msg,sizeof(msg),0,(struct sockaddr *) &sin, &structlength);
			if ( recvd < 0 ) 
			{
				perror("recvfrom");
				exit(1);
			}
			if (recvd > 0) 
			{
				printf("got something.");
				char tempchar[10] = ACK;
				if ( strcmp(msg,tempchar) == 0)
				{
					noACK = 0;
					tempint = -1;
					if ( sendto(sd2, &tempint,sizeof(tempint),0,(struct sockaddr *) &pin, sizeof(pin)) < 0 )
					{
						perror("sendto");
						exit(1);
					}
				}

			}
			else
				printf(".");
		}
	}
	close(sd);
	sleep(1);
	return 0;
}
void swap(int *a, int *b)
{
  int t=*a; *a=*b; *b=t;
}

void sort(int arr[], int beg, int end)
{
  if (end > beg + 1)
  {
    int piv = arr[beg], l = beg + 1, r = end;
    while (l < r)
    {
      if (arr[l] <= piv)
        l++;
      else
        swap(&arr[l], &arr[--r]);
    }
    swap(&arr[--l], &arr[beg]);
    sort(arr, beg, l);
    sort(arr, r, end);
  }
}
