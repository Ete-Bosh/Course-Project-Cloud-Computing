# Course-Project-Cloud-Computing
A codebase for course project - RDMA multicast with WRITE.

Currently I have a baseline for RDMA multicast with UD QP. Run sender.c and receiver.c for further information. I reserved Bluefield 2 DPU on cloudlab and it is unavailable until Nov 8, 2025 8:00 AM. So all functions with DOCA cannot be run on hosts without DPU. I'll share updates when I get results from incoming updates.

If you don't have host with RNIC like Mellanox series, you can resort to (this project)[https://github.com/Ete-Bosh/RDMA-toy] to install soft-roce. Some codes are shared with other projects so I cannot publish them here. If you have any question, please send me an email to jwan20@cs.jhu.edu and I am glad to help you.
