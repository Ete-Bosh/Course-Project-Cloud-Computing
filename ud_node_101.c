#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <time.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h> // for size_t

#define MAX_RC_CONNS 2
#define CQ_LEN 10
#define QKEY 0x12121212
#define MULTICAST_ADDR "239.255.0.1"
#define MULTICAST_MLID 0
#define MULTICAST_QPN 0xFFFFFF
#define RDMA_PORT  "23502"

char local_ip[INET_ADDRSTRLEN] = "10.10.1.2"; // placeholder

struct RcConn {
    struct rdma_cm_id *id;   // child cm_id for this connection
    struct ibv_qp *qp;       // RC QP bound to child id (belongs to your PD/CQ)
    int active;              // 1 if accepted and live
};

struct Context{
    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;
    struct ibv_context *ctx;
    
    struct ibv_pd *pd;
    struct ibv_mr *mr;

    struct ibv_cq *cq;
    struct ibv_qp *qp;

    struct ibv_ah *ah;
    struct rdma_cm_id *listen_id;
    struct RcConn rc[MAX_RC_CONNS];
};
struct Context *context;

int init(struct Context *context, char* buffer, int buffer_len){
    int ret;

    /* 0. Init device */
    int num_device = 0;
    context->dev_list = ibv_get_device_list(&num_device);
    if(!context->dev_list || !num_device){
        fprintf(stderr, "No IB devices found\n");
        return 1;
    }
    for(int i = 0; i < num_device; i++){
        printf("%s\n",context->dev_list[i]->name);
        if(strcmp(context->dev_list[i]->name, "rocep19s0") == 0){
	    context->ib_dev = context->dev_list[i];
	    printf("Device number: %d\n",i);
	}
    }

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
	//printf("PD Created\n");
    /* 2. Register MR*/
    context->mr = ibv_reg_mr(context->pd, buffer, buffer_len, IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
				 IBV_ACCESS_LOCAL_WRITE);
    if(!context->mr){
        fprintf(stderr, "Failed to register memory, errno: %d\n", errno);
        return 1;
    }
	//printf("MR Created\n");
    /* 3. Create CQ*/
    context->cq = ibv_create_cq(context->ctx, CQ_LEN, NULL, NULL, 0);
    if(!context->cq){
        fprintf(stderr, "Failed to create CQ\n");
        return 1;
    }
	//printf("CQ Created\n");
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
    }
	

    /* 8. Create AH*/
    {
        int gid_index = -1;
        for (int i = 0; i < 16; i++) {
            struct ibv_gid_entry entry;
            if (ibv_query_gid_ex(context->ctx, 1, i, &entry, 0) == 0) {
                printf("[Sender] GID index %d: ", i);
                for (int j = 0; j < 16; j++) {
                    printf("%02x", entry.gid.raw[j]);
                    if(j < 15) printf(":");
                }
                printf("\n");
		printf("type = %u\n", entry.gid_type);
                // If non-zero GID found, use it
		//printf("%d %d %d\n",entry.gid.raw[10] == 0xff, entry.gid.raw[11] == 0xff, entry.gid_type == 2);
                if (entry.gid.raw[10] == 0xff && entry.gid.raw[11] == 0xff && entry.gid_type == 2) {
                    gid_index = i;
		    printf("[Sender] Using GID index %d\n", gid_index);
                    
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
        printf("[Sender] Sending to multicast GID: ");
        for (int i = 0; i < 16; i++) {
            printf("%02x", mgid.raw[i]);
            if (i % 2 == 1) printf(":");
        }
        printf("\n");
    }
}
int rc_accept_once(struct Context *c){
    int ret;
    struct rdma_cm_id *child = NULL;

    ret = rdma_get_request(c->listen_id, &child);
    if (ret) { perror("rdma_get_request"); return -1; }

    // Sanity: should be same device (verbs) as your ctx
    if (child->verbs != c->ctx) {
        fprintf(stderr, "[RC] ERROR: child->verbs != context->ctx (different device)\n");
        rdma_destroy_id(child);
        return -1;
    }

    // Create RC QP for this child, explicitly reusing your PD & CQ
    struct ibv_qp_init_attr qattr;
    memset(&qattr, 0, sizeof(qattr));
    qattr.qp_type = IBV_QPT_RC;
    qattr.send_cq = c->cq;
    qattr.recv_cq = c->cq;
    qattr.cap.max_send_wr  = CQ_LEN;
    qattr.cap.max_recv_wr  = CQ_LEN;
    qattr.cap.max_send_sge = 1;
    qattr.cap.max_recv_sge = 1;

    ret = rdma_create_qp(child, c->pd, &qattr); // <— REUSE PD here
    if (ret) {
        perror("rdma_create_qp(child)");
        rdma_destroy_id(child);
        return -1;
    }

    // Accept (kernel will drive to RTR/RTS using CM)
    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));

    ret = rdma_accept(child, &param);
    if (ret) {
        perror("rdma_accept");
        rdma_destroy_qp(child);
        rdma_destroy_id(child);
        return -1;
    }

    // Find a free slot
    int idx = -1;
    for (int i=0;i<8;i++){ if (!c->rc[i].active) { idx = i; break; } }
    if (idx < 0) {
        fprintf(stderr, "[RC] No free slots left\n");
        // polite close
        rdma_disconnect(child);
        rdma_destroy_qp(child);
        rdma_destroy_id(child);
        return -1;
    }

    c->rc[idx].id = child;
    c->rc[idx].qp = child->qp;
    c->rc[idx].active = 1;

    printf("[RC] Accepted connection in slot %d (QPN=%u)\n", idx, c->rc[idx].qp->qp_num);
    return idx;
}

void sender_init(struct Context *context, char* buff, int &len){
    len = 2;
    buff = malloc(len);
    *buff = 13;
    *(buff + 1) = 37;

    init(context, buff, len);

    // 1. Push the listener
    {
        struct rdma_addrinfo hints, *res;
        struct ibv_qp_init_attr init_attr;
        struct ibv_qp_attr qp_attr;

        memset(&hints, 0, sizeof hints);
        hints.ai_flags = RAI_PASSIVE | RAI_NUMERICHOST;
        hints.ai_port_space = RDMA_PS_TCP;
        ret = rdma_getaddrinfo(local_ip, RDMA_PORT, &hints, &res);
        if (ret) {
            perror("rdma_getaddrinfo failed");
            return ret;
        }

        memset(&init_attr, 0, sizeof init_attr);
        
        //For debug only
        init_attr.qp_type = IBV_QPT_RC;
        init_attr.send_cq = context->cq; // reuse your CQ
        init_attr.recv_cq = context->cq;
        init_attr.cap.max_send_wr = init_attr.cap.max_recv_wr = 1;
        init_attr.cap.max_send_sge = init_attr.cap.max_recv_sge = 1;
        init_attr.cap.max_inline_data = 64;
        init_attr.sq_sig_all = 1;
        ret = rdma_create_ep(context->listen_id, res, NULL, &init_attr);
        if (ret) {
            perror("rdma_create_ep");
        }

        ret = rdma_listen(context->listen_id, 2);
        if (ret) {
            perror("rdma_listen");
        }
        puts("Listen successfully");
    }

    int idx0 = rc_accept_once(context); // blocks until a connection arrives
    if (idx0 < 0) { fprintf(stderr, "accept #1 failed\n"); return 1; }

    int idx1 = rc_accept_once(context); // accept a second
    if (idx1 < 0) { fprintf(stderr, "accept #2 failed\n"); return 1; }

}
void recver_init(struct Context *context, char* buff,int &len)
{
    len = 1 + 100;
    buff = malloc(len);

    init(context, buff, len);
    // 2. Connect to right node
    {
        sleep(1); // Sleep 1 seconds for listen
        struct rdma_addrinfo hints, *res;
        struct ibv_qp_init_attr attr;

        memset(&hints, 0, sizeof hints);
        hints.ai_port_space = RDMA_PS_TCP;
        ret = rdma_getaddrinfo(right_ip, RDMA_PORT, &hints, &res);
        memset(&attr, 0, sizeof attr);
        attr.cap.max_send_wr = attr.cap.max_recv_wr = 1;
        attr.cap.max_send_sge = attr.cap.max_recv_sge = 1;
        attr.cap.max_inline_data = 64;
        attr.qp_context = right_id;
        attr.sq_sig_all = 1;
        ret = rdma_create_ep(&right_id, res, NULL, &attr);
        if (ret) {
            perror("rdma_create_ep");
            
        }

        right_mr = rdma_reg_msgs(right_id, messages[0], sizeof(messages[0]));
        if (!right_mr) {
            perror("rdma_reg_msgs for recv_msg");
            ret = -1;
            
        }
        ret = rdma_post_recv(right_id, (void *)(uintptr_t)0, &messages[0], sizeof(struct message), right_mr);
        ret = rdma_connect(right_id, NULL);
        if (ret) {
            perror("rdma_connect"); 
        }
        puts("Connect successfully");


        //////////////////////////////////////////////////////////////
        ret = rdma_get_request(listen_id, &left_id);
        if (ret) {
            perror("rdma_get_request");
        }

        left_mr = rdma_reg_msgs(left_id, messages[1], sizeof(messages[1]));
        if (!left_mr) {
            ret = -1;
            perror("rdma_reg_msgs");
        }

        ret = rdma_post_recv(left_id, (void *)(uintptr_t)1, &messages[1], sizeof(struct message), left_mr);

        ret = rdma_accept(left_id, NULL);
        if (ret) {
            perror("rdma_accept");
        }
        puts("Accept successfully");

    }
}
int main(int argc, char **argv)
{
    char *buffer;
    int len;
    
    context = (struct Context*)malloc(sizeof(struct Context));
    memset(context, 0, sizeof(struct Context));

    return 0;
}