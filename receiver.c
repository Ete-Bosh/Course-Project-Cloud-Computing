/*********
 * 
 * Caution: 
 * 1. 32-bit may not be enough for large file
 * 2. We use dev_list[2] for fns101
 * 
 * 
 * 
 *********/


#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <netdb.h>
#include <sys/socket.h>
#include <signal.h>
#include <stdint.h>  

#define BATCH 30
#define CQ_LEN 2048
#define QKEY 0x12121212
#define MULTICAST_ADDR "239.255.0.1"
#define MULTICAST_MLID 0
#define MULTICAST_QPN 0xFFFFFF
#define ACK_MSG_SIZE 4
#define CONTROL_PORT "23500"
#define BIT_TEST(a,i)  ( (a[(i)>>3]) &  (1u << ((i)&7)) )
#define BIT_SET(a,i)   (  a[(i)>>3]  |= (1u << ((i)&7)) )

// RDMA device selection, 0 for first one, 2 for when using on fns101
int num_device = 0;

int recv_buffer_size;
uint32_t expected_total_bytes;
int expected_total_packets;

char port[16];
char sender_ip[INET6_ADDRSTRLEN]; 
int send_buffer_len;
int recv_buffer_len;
char *send_buff = NULL;
char *recv_buff = NULL;
volatile size_t total = 0;

//Retransmission
uint8_t  *pkt_bitmap   = NULL;      /* 1 bit per packet               */
size_t    bitmap_size  = 0;         /* bytes in pkt_bitmap            */
volatile size_t missing_count = 0;  /* packets still missing          */
volatile int    stop_received = 0;  /* set after first STOP marker    */

struct rdma_ack_context {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_mr *mr;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_chan;
    unsigned char *send_buffer;
    size_t buffer_size;
};

struct Context{
    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;
    struct ibv_context *ctx;
    
    struct ibv_pd *pd;
    struct ibv_mr *mr_send;
    struct ibv_mr *mr_recv;

    struct ibv_cq *cq;
    struct ibv_qp *qp;

    struct ibv_ah *ah;
};
 
// Print total bytes received upon force quit
void handle_sigint(int sig) {
    (void)sig;
    printf("\n[Receiver] Caught SIGINT (Ctrl+C)\n");
    printf("[Receiver] Total Received Bytes: %zu\n", total);
    exit(0);
}
// Extract incoming IP from TCP 
void *get_in_addr(struct sockaddr *sa) {
    if (sa->sa_family == AF_INET)
        return &(((struct sockaddr_in *)sa)->sin_addr);
    else
        return &(((struct sockaddr_in6 *)sa)->sin6_addr);
}
void receive_control_info() {
    int sockfd, new_fd;
    struct addrinfo hints, *res;
    struct sockaddr_storage their_addr;
    socklen_t addr_size;
    char buffer[128];

    printf("[Receiver] Waiting for control message on TCP port %s...\n", CONTROL_PORT);

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if (getaddrinfo(NULL, CONTROL_PORT, &hints, &res) != 0) {
        perror("[Receiver] getaddrinfo");
        exit(1);
    }

    sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sockfd < 0) {
        perror("[Receiver] socket");
        exit(1);
    }

    int yes = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

    if (bind(sockfd, res->ai_addr, res->ai_addrlen) < 0) {
        perror("[Receiver] bind");
        close(sockfd);
        exit(1);
    }

    listen(sockfd, 1);
    addr_size = sizeof their_addr;
    new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &addr_size);
	
	// Convert sender's IP to string
	inet_ntop(their_addr.ss_family,
			  get_in_addr((struct sockaddr *)&their_addr),
			  sender_ip, sizeof(sender_ip));
			  
    memset(buffer, 0, sizeof(buffer));
    recv(new_fd, buffer, sizeof(buffer) - 1, 0);
    //printf("[Receiver] Received control message: %s\n", buffer);
    printf("[Receiver] Received control signal.\n");

    // Parse values
    sscanf(buffer, "%d %u %s", &recv_buffer_size, &expected_total_bytes, port);
	send_buffer_len = recv_buffer_size * BATCH;
	recv_buffer_len = (recv_buffer_size + 100) * BATCH;
	
	// Allocate send & recv buffer
	send_buff = malloc(send_buffer_len);
	recv_buff = malloc(recv_buffer_len);

	// Sanity check
	if (!send_buff || !recv_buff) {
		fprintf(stderr, "Failed to allocate send/recv buffers.\n");
		exit(EXIT_FAILURE);
	}
	
	// Get expected total number of packets
	expected_total_packets = (expected_total_bytes + (recv_buffer_size - 5)) / (recv_buffer_size);

    /* Allocate bitmap (1 bit per packet) */
    bitmap_size   = (expected_total_packets + 7) / 8;
    pkt_bitmap    = calloc(bitmap_size, 1);
    missing_count = expected_total_packets;
    if (!pkt_bitmap) { perror("calloc bitmap"); exit(EXIT_FAILURE); }

	
    printf("[Receiver] Received values:\n");
    printf("  RECV_BUFFER_SIZE = %d\n", recv_buffer_size);
    printf("  EXPECTED Payload Bytes = %d\n", expected_total_bytes);
    printf("  EXPECTED Total Packets = %d\n", expected_total_packets);
    printf("  RDMA/RoCE PORT = %s\n", port);
	printf("  Connection from sender IP: %s\n", sender_ip);
	printf("  SEND_BUFFER_LEN = %d\n", send_buffer_len);
	printf("  RECV_BUFFER_LEN = %d\n", recv_buffer_len);
	
	
    close(new_fd);
    close(sockfd);
    freeaddrinfo(res);
}
/* Send one UDP datagram per missing sequence number to CONTROL_PORT */
static void request_missing_packets(void)
{
    if (missing_count == 0) return;

    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) { perror("socket"); return; }

    struct sockaddr_in dst = {0};
    dst.sin_family = AF_INET;
    dst.sin_port   = htons(atoi(CONTROL_PORT));
    if (inet_pton(AF_INET, sender_ip, &dst.sin_addr) != 1) {
        perror("inet_pton"); close(s); return;
    }

    for (uint32_t seq = 0; seq < (uint32_t)expected_total_packets; ++seq) {
        if (!BIT_TEST(pkt_bitmap, seq)) {
            sendto(s, &seq, sizeof(seq), 0,
                   (struct sockaddr *)&dst, sizeof(dst));
        }
    }
    close(s);
    printf("[Receiver] Requested retransmission for %zu missing packets.\n",
           missing_count);
}
int init_rdma_ack_context(struct rdma_ack_context *ctx, const char *sender_ip, const char *port, size_t buffer_size) {
    struct addrinfo hints = {0}, *res;
    struct sockaddr_in src_addr = {0};
    struct rdma_cm_event *event = NULL;
    struct rdma_conn_param conn_param = {0};
    int ret;

    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;

    ctx->buffer_size = buffer_size;
    ctx->send_buffer = malloc(buffer_size);
    if (!ctx->send_buffer) {
        perror("send_buffer malloc failed");
        return 1;
    }
    memset(ctx->send_buffer, 0xAC, buffer_size);

    ctx->ec = rdma_create_event_channel();
    if (!ctx->ec) {
        perror("rdma_create_event_channel");
        return 1;
    }

    ret = rdma_create_id(ctx->ec, &ctx->id, NULL, RDMA_PS_TCP);
    if (ret) {
        perror("rdma_create_id");
        return 1;
    }

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    getaddrinfo(sender_ip, port, &hints, &res);

    src_addr.sin_family = AF_INET;
    src_addr.sin_addr.s_addr = INADDR_ANY;
    src_addr.sin_port = 0;

    ret = rdma_resolve_addr(ctx->id, (struct sockaddr *)&src_addr, res->ai_addr, 2000);
    if (ret) {
        perror("rdma_resolve_addr");
        return 1;
    }

    rdma_get_cm_event(ctx->ec, &event);
    rdma_ack_cm_event(event);

    ret = rdma_resolve_route(ctx->id, 2000);
    if (ret) {
        perror("rdma_resolve_route");
        return 1;
    }

    rdma_get_cm_event(ctx->ec, &event);
    rdma_ack_cm_event(event);

    ctx->pd = ibv_alloc_pd(ctx->id->verbs);
    ctx->mr = ibv_reg_mr(ctx->pd, ctx->send_buffer, buffer_size, IBV_ACCESS_LOCAL_WRITE);

    ctx->comp_chan = ibv_create_comp_channel(ctx->id->verbs);
    ctx->cq = ibv_create_cq(ctx->id->verbs, 10, NULL, ctx->comp_chan, 0);
    ibv_req_notify_cq(ctx->cq, 0);

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = ctx->cq,
        .recv_cq = ctx->cq,
        .cap = {.max_send_wr = 1, .max_recv_wr = 1, .max_send_sge = 1, .max_recv_sge = 1},
        .qp_type = IBV_QPT_RC
    };

    ret = rdma_create_qp(ctx->id, ctx->pd, &qp_attr);
    if (ret) {
        perror("rdma_create_qp");
        return 1;
    }

    ctx->id->send_cq = ctx->cq;
    ctx->id->recv_cq = ctx->cq;

    conn_param.initiator_depth = 1;
    conn_param.responder_resources = 1;

    ret = rdma_connect(ctx->id, &conn_param);
    if (ret) {
        perror("rdma_connect failed");
        return 1;
    }

    rdma_get_cm_event(ctx->ec, &event);
    if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
        fprintf(stderr, "Expected ESTABLISHED event, got %d\n", event->event);
        rdma_ack_cm_event(event);
        return 1;
    }
    rdma_ack_cm_event(event);

    // Optional: Debug QP state
    ret = ibv_query_qp(ctx->id->qp, &attr, IBV_QP_STATE, &init_attr);
    if (ret == 0) {
        printf("QP state: %d (expect 4 for RTS)\n", attr.qp_state);
    } else {
        perror("ibv_query_qp");
    }

    freeaddrinfo(res);
    return 0;
}


int send_final_ack(struct rdma_ack_context *ctx) {
    struct ibv_send_wr send_wr = {0}, *bad_wr;
    struct ibv_sge sge;
    struct ibv_wc wc;
    struct ibv_cq *ev_cq;
    void *ev_ctx;

    sge.addr = (uintptr_t)ctx->send_buffer;
    sge.length = ctx->buffer_size;
    sge.lkey = ctx->mr->lkey;

    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;


	/*
	printf("DEBUG: id = %p, qp = %p\n", ctx->id, ctx->id ? ctx->id->qp : NULL);
	printf("DEBUG: mr = %p\n", ctx->mr);
	printf("DEBUG: send_buffer = %p\n", ctx->send_buffer);
	printf("DEBUG: lkey = %u\n", ctx->mr ? ctx->mr->lkey : 0);
	printf("DEBUG: id->verbs = %p\n", ctx->id ? ctx->id->verbs : NULL);
	struct ibv_qp_attr attr;
	struct ibv_qp_init_attr init_attr;
	int ret = ibv_query_qp(ctx->id->qp, &attr, IBV_QP_STATE, &init_attr);
	if (ret == 0) {
		printf("QP state: %d\n", attr.qp_state);  // Expect 4 (RTS)
	} else {
		perror("ibv_query_qp failed");
	}
	*/
    if (ibv_post_send(ctx->id->qp, &send_wr, &bad_wr)) {
        perror("ibv_post_send failed");
        return 1;
    }

    ibv_get_cq_event(ctx->comp_chan, &ev_cq, &ev_ctx);
    ibv_ack_cq_events(ctx->cq, 1);
    ibv_req_notify_cq(ctx->cq, 0);
    ibv_poll_cq(ctx->cq, 1, &wc);

    if (wc.status == IBV_WC_SUCCESS) {
        printf("ACK sent successfully.\n");
    } else {
        fprintf(stderr, "Send failed: %s\n", ibv_wc_status_str(wc.status));
        return 1;
    }

    return 0;
}
void cleanup_rdma_ack_context(struct rdma_ack_context *ctx) {
    rdma_disconnect(ctx->id);
    rdma_destroy_qp(ctx->id);
	ibv_dereg_mr(ctx->mr);
	ibv_dealloc_pd(ctx->pd);
	free(ctx->send_buffer);
	rdma_destroy_id(ctx->id);
	rdma_destroy_event_channel(ctx->ec);
	ibv_destroy_comp_channel(ctx->comp_chan);
}

int init(struct Context *context){
    int ret;

    /* 0. Init device */
    context->dev_list = ibv_get_device_list(&num_device);
    if(!context->dev_list || !num_device){
        fprintf(stderr, "No IB devices found\n");
        return 1;
    }

    context->ib_dev = context->dev_list[0];
    context->ctx = ibv_open_device(context->ib_dev);
    if (!context->ctx) {
        fprintf(stderr, "Failed to open device\n");
        return 1;
    }

    /* 1. Allocate PD */
    context->pd = ibv_alloc_pd(context->ctx);
    if (!context->pd) {
        fprintf(stderr, "Failed to allocate PD\n");
        return 1;
    }

    /* 2. Register MR*/
    context->mr_send = ibv_reg_mr(context->pd, send_buff, send_buffer_len, IBV_ACCESS_LOCAL_WRITE);
    if(!context->mr_send){
        fprintf(stderr, "Failed to register memory, errno: %d\n", errno);
        return 1;
    }
    context->mr_recv = ibv_reg_mr(context->pd, recv_buff, recv_buffer_len, IBV_ACCESS_LOCAL_WRITE);
    if(!context->mr_recv){
        fprintf(stderr, "Failed to register memory, errno: %d\n", errno);
        return 1;
    }

    /* 3. Create CQ*/
    context->cq = ibv_create_cq(context->ctx, CQ_LEN, NULL, NULL, 0);
    if(!context->cq){
        fprintf(stderr, "Failed to create CQ\n");
        return 1;
    }

    /* 5. Create UP QP*/
    {
        struct ibv_qp_init_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_type          = IBV_QPT_UD;
        attr.send_cq          = context->cq;
        attr.recv_cq          = context->cq;
        attr.cap.max_send_wr  = CQ_LEN;
        attr.cap.max_recv_wr  = CQ_LEN;
        attr.cap.max_send_sge = 1;
        attr.cap.max_recv_sge = 1;
        context->qp = ibv_create_qp(context->pd, &attr);
        if(!context->qp){
            fprintf(stderr, "Failed to create QP\n");
            return 1;
        }
    }

    /* 6. Transit QP states*/
    {
        struct ibv_qp_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.pkey_index = 0;
        attr.port_num   = 1;
        attr.qkey       = QKEY;
        
        ret = ibv_modify_qp(context->qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                                                IBV_QP_PORT  | IBV_QP_QKEY);
        if(ret){
            fprintf(stderr, "Failed to modify QP to INIT, ret = %d\n", ret);
            return 1;
        }
    
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTR;
        ret = ibv_modify_qp(context->qp, &attr, IBV_QP_STATE);
        if (ret) {
            fprintf(stderr, "Failed to modify QP to RTR, ret = %d\n", ret);
            return 1;
        }

        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTS;
        attr.sq_psn   = 0;
        ret = ibv_modify_qp(context->qp, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN);
        if (ret) {
            fprintf(stderr, "Failed to modify QP to RTS, ret = %d\n", ret);
            return 1;
        }
    }


    /* 7. Join multicast group*/
    {
        union ibv_gid mgid;
        memset(&mgid, 0, sizeof(mgid));
        mgid.raw[10] = 0xff;
        mgid.raw[11] = 0xff;

        if (inet_pton(AF_INET, MULTICAST_ADDR, &mgid.raw[12]) != 1) {
            perror("inet_pton");
            return 1;
        }
        
        if(ibv_attach_mcast(context->qp, &mgid, MULTICAST_MLID)){
            perror("Failed to attach to multicast group");
            return 1;
        }
        
        printf("Joined multicast group: %s\n", MULTICAST_ADDR);  
    }

    /* 8. Create AH*/
    {
        int gid_index = -1;
        for (int i = 0; i < 16; i++) {
            union ibv_gid gid;
            if (ibv_query_gid(context->ctx, 1, i, &gid) == 0) {
                printf("GID index %d: ", i);
                for (int j = 0; j < 16; j++) {
                    printf("%02x", gid.raw[j]);
                    if(j < 15) printf(":");
                }
                printf("\n");
                // If non-zero GID found, use it
                if (gid.raw[10] == 0xff && gid.raw[11] == 0xff) {
                    gid_index = i;
                    printf("Using GID index %d\n", gid_index);
                    break;
                }
            }
        }

        if (gid_index == -1) {
            fprintf(stderr, "Failed to find valid GID index\n");
            return 1;
        }

        union ibv_gid mgid;
        memset(&mgid, 0, sizeof(mgid));
        mgid.raw[10] = 0xff;
        mgid.raw[11] = 0xff;
        if (inet_pton(AF_INET, MULTICAST_ADDR, &mgid.raw[12]) != 1) {
            perror("inet_pton");
            return 1;
        }

        struct ibv_ah_attr ah_attr;
        memset(&ah_attr, 0, sizeof(ah_attr));
        ah_attr.is_global = 1;
        ah_attr.dlid = 0;
        ah_attr.sl = 0;
        ah_attr.port_num = 1;
        ah_attr.grh.hop_limit = 64;
        ah_attr.grh.sgid_index = gid_index;
        ah_attr.grh.dgid = mgid;

        context->ah = ibv_create_ah(context->pd, &ah_attr);
        if (!context->ah) {
            fprintf(stderr, "Failed to create Address Handle for multicast\n");
            return 1;
        }
        printf("Sending to multicast GID: ");
        for (int i = 0; i < 16; i++) {
            printf("%02x", mgid.raw[i]);
            if (i % 2 == 1) printf(":");
        }
        printf("\n");
    }

}
 
void clean(struct Context *context){

    ibv_free_device_list(context->dev_list);
    ibv_destroy_ah(context->ah);
    ibv_dereg_mr(context->mr_send);
    ibv_dereg_mr(context->mr_recv);
    ibv_destroy_qp(context->qp);
    ibv_destroy_cq(context->cq);
    ibv_dealloc_pd(context->pd);
    ibv_close_device(context->ctx);
}
 
int receive_multicast(struct Context *context, struct rdma_ack_context *ack_ctx){


    struct ibv_sge      sge[BATCH];
    struct ibv_recv_wr  wr[BATCH], *bad_wr = NULL;
    memset(sge, 0, sizeof(sge));
    memset(wr,  0, sizeof(wr));

    for(int i = 0; i < BATCH; i++){
        sge[i].addr = (uintptr_t)(recv_buff + i * (recv_buffer_size + 100));
        sge[i].length = recv_buffer_size + 100;
        sge[i].lkey = context->mr_recv->lkey;

        wr[i].wr_id = i;
        wr[i].sg_list = &sge[i];
        wr[i].num_sge = 1;
        
        if(i == BATCH - 1){
            wr[i].next = NULL;
        }else{
            wr[i].next = &wr[i + 1];
        }
    }

    if (ibv_post_recv(context->qp, &wr[0], &bad_wr)) {
        perror("Failed to post recv wr");
        return 1;
    }

    printf("Waiting for a completion\n");

    struct ibv_wc wc[BATCH];
	int total_pkt_rec = 0;
    //int total = 0;
    while(1){
       int n = 0;
        while((n = ibv_poll_cq(context->cq, BATCH, &wc[0])) == 0){
           /* Waiting for a completion */
        }
        
        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr,"recv err %s\n",
                        ibv_wc_status_str(wc[i].status));
                continue;
            }
            
            // offset in the buffer
            int idx = wc[i].wr_id;
            
            char *payload = recv_buff + idx * (recv_buffer_size + 100);
            char *data = payload + 40;
            int length = wc[i].byte_len - 40;
			//printf("Received %d bytes, first 4 bytes: 0x%08X\n", length, *(uint32_t*)data);
            if (*(uint32_t *)data == 0xFFFFFFFF) {
                /* STOP marker arrived */
                stop_received = 1;
                if (missing_count == 0) {
                    //send_final_ack();
					send_final_ack(ack_ctx);
                    goto done;
                } else {
                    request_missing_packets();
                }
                continue;         /* do NOT count the STOP packet */
            }

            // Normal data packet
            uint32_t seq = *(uint32_t *)data;
            if (seq < (uint32_t)expected_total_packets && !BIT_TEST(pkt_bitmap, seq)) {
                BIT_SET(pkt_bitmap, seq);
                --missing_count;
            }
            total += length;        /* track total bytes          */

            if (stop_received && missing_count == 0) {
                printf("[Receiver] All packets present after retransmissions.\n");
                //send_final_ack();
				send_final_ack(ack_ctx);
                goto done;
            }
            //process_chunk(data, length);
            total += length;
			++total_pkt_rec;
			//printf("Total Received Packets: %d\n", total_pkt_rec);
            //printf("%dB received\n",total);
           
        }
         
        //repost in one doorbell
        for (int i = 0; i < n; ++i) {
            int idx = wc[i].wr_id;
            //sge[idx].addr = (uintptr_t)(recv_buff + idx * (CHUNK + 100));   
            wr[idx].next  = (i == n-1) ? NULL : &wr[wc[i+1].wr_id];
        }
        if (ibv_post_recv(context->qp, &wr[wc[0].wr_id], &bad_wr)) {
            perror("Failed to post recv wr");
            return 1;
        } 
         
    }
done:
	//free(send_buffer);
	free(pkt_bitmap);
    clean(context);
	cleanup_rdma_ack_context(ack_ctx);
}
 
int main(){
	signal(SIGINT, handle_sigint);
	receive_control_info();
    struct Context context;
	struct rdma_ack_context ack_ctx;
    memset(&context, 0, sizeof(context));
	memset(&ack_ctx, 0, sizeof(ack_ctx));
    init(&context);
	init_rdma_ack_context(&ack_ctx, sender_ip, port, recv_buffer_size);
    receive_multicast(&context, &ack_ctx);
    return 0;
}
 
