// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replica/main.cc:
 *   test replica application; "null RPC" equivalent
 * 
 * Copyright 2021 Aishwarya Ganesan and Ramnatthan Alagappan
 *
 * Small changes made to the code to implement Skyros
 *
 * *************************************************************
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "lib/configuration.h"
#include "common/replica.h"
#include "lib/udptransport.h"
#include "lib/fasttransport.h"
#include "vr/replica.h"
#include "vr/durabilityreplica.h"

#include <unistd.h>
#include <stdlib.h>
#include <vector>
#include <fstream>
#include <iostream>

#include <unistd.h>
#include <sched.h>
#include <thread>
#include <csignal>


static void
Usage(const char *progName)
{
        fprintf(stderr, "usage: %s -c conf-file [-R] -i replica-index -m vr [-b batch-size] [-d packet-drop-rate] [-r packet-reorder-rate] [-q dscp]\n",
                progName);
        exit(1);
}

static bool g_terminate_program = false;

void signal_handler(int signal) {
    if (signal == SIGTERM || signal == SIGINT) {
        g_terminate_program = true;
    }
}


int
main(int argc, char **argv)
{
    int index = -1;
    const char *configPath = NULL;
    double dropRate = 0.0;
    double reorderRate = 0.0;
    int dscp = 0;
    int batchSize = 1;
    bool recover = false;
    int totalThreads = 15;

    auto buffer_size = 0;

    // Set the signal handlers for SIGINT and SIGTERM signals
    // std::signal(SIGINT, signal_handler);
    // std::signal(SIGTERM, signal_handler);

    specpaxos::AppReplica *nullApp = new specpaxos::AppReplica();

    // while (!g_terminate_program) {
    enum
    {
        PROTO_UNKNOWN,
        PROTO_VR        
    } proto = PROTO_UNKNOWN;

    // Parse arguments
    int opt;
    while ((opt = getopt(argc, argv, "b:c:d:i:m:q:r:l:j:R:tw:")) != -1) {
        switch (opt) {
        case 'b':
        {
            char *strtolPtr;
            batchSize = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0')
                || (batchSize < 1))
            {
                fprintf(stderr,
                        "option -b requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'c':
            configPath = optarg;
            break;

        case 'd':
        {
            char *strtodPtr;
            dropRate = strtod(optarg, &strtodPtr);
            if ((*optarg == '\0') || (*strtodPtr != '\0') ||
                ((dropRate < 0) || (dropRate >= 1))) {
                fprintf(stderr,
                        "option -d requires a numeric arg between 0 and 1\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'i':
        {
            char *strtolPtr;
            index = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (index < 0))
            {
                fprintf(stderr,
                        "option -i requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'm':
            if (strcasecmp(optarg, "vr") == 0) {
                proto = PROTO_VR;
            } else {
                fprintf(stderr, "unknown mode '%s'\n", optarg);
                Usage(argv[0]);
            }
            break;

        case 'q':
        {
            char *strtolPtr;
            dscp = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (dscp < 0))
            {
                fprintf(stderr,
                        "option -q requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'r':
        {
            char *strtodPtr;
            reorderRate = strtod(optarg, &strtodPtr);
            if ((*optarg == '\0') || (*strtodPtr != '\0') ||
                ((reorderRate < 0) || (reorderRate >= 1))) {
                fprintf(stderr,
                        "option -r requires a numeric arg between 0 and 1\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'j':
        {
            char *strtolPtr;
            totalThreads = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -z requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'R':
            recover = true;
            break;
        case 'l':
        {
            char *strtolPtr;
            buffer_size = strtoul(optarg, &strtolPtr, 10);
            break;
        }
        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            Usage(argv[0]);
            break;
        }
    }

    if (!configPath) {
        fprintf(stderr, "option -c is required\n");
        Usage(argv[0]);
    }
    if (index == -1) {
        fprintf(stderr, "option -i is required\n");
        Usage(argv[0]);
    }
    if (proto == PROTO_UNKNOWN) {
        fprintf(stderr, "option -m is required\n");
        Usage(argv[0]);
    }
    if ((proto != PROTO_VR) && (batchSize != 1)) {
        Warning("Batching enabled, but has no effect on non-VR protocols");
    }

    // Load configuration
    std::ifstream configStream(configPath);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read internal configuration file: %s\n",
                configPath);
        Usage(argv[0]);
    }
    specpaxos::Configuration internalConfig(configStream);


    if (index >= internalConfig.n) {
        fprintf(stderr, "replica index %d is out of bounds; "
                "only %d replicas defined\n", index, internalConfig.n);
        Usage(argv[0]);
    }

    // Some hardware has limit of x of queues for rpc (16 for xl170)
    //uint8_t numa_node = (i % 4 < 2)?0:1;
    uint8_t numa_node = 0;
    const size_t num_socket_cores = erpc::get_lcores_for_numa_node(numa_node).size();        
    auto ht_ct = 0;

    //UDPTransport internalTransport(dropRate, reorderRate, dscp, nullptr);
    constexpr std::size_t BUFFER_SIZE_MULTIPLIER = 1024 * 4;
    auto send_buffer_size = 0;
    auto recv_buffer_size = 0;
    if (buffer_size == 0)
    {
        if (index == 0)
        {
            send_buffer_size = BUFFER_SIZE_MULTIPLIER * (batchSize / 64);
        }
        else
        {
            recv_buffer_size = BUFFER_SIZE_MULTIPLIER * (batchSize / 64);
        }
    }
    else
    {
        if (index == 0)
        {
            send_buffer_size = buffer_size;
        }
        else
        {
            recv_buffer_size = buffer_size;
        }
    }
    auto physical_port = 0x0;
    FastTransport internalTransport(internalConfig, ht_ct, 1, physical_port, numa_node, 0, send_buffer_size, recv_buffer_size);
    specpaxos::Replica *replica;
    assert(proto == PROTO_VR);

    std::ifstream clientConfigStream(std::string(configPath) + ".client");
        if (clientConfigStream.fail()) {
            fprintf(stderr, "unable to read client configuration file: %s\n",
                    (std::string(configPath) + ".client").c_str());
            Usage(argv[0]);
        }

    specpaxos::Configuration clientConfig(clientConfigStream);

    assert(ht_ct < num_socket_cores);

    auto& internal_transport = internalTransport;
    std::atomic<bool> internal_transport_thread_ready = false;
    std::thread internal_transport_thread([&]()
    {
        replica = new specpaxos::vr::VRReplica(internalConfig, index,
                                            !recover,
                                            &internalTransport,
                                            batchSize,
                                            nullApp);
        internal_transport_thread_ready = true;

        internal_transport.Run();
    });
    erpc::bind_to_core(internal_transport_thread, numa_node, 0);
    {
        int policy;
        struct sched_param param;
        pthread_getschedparam(internal_transport_thread.native_handle(), &policy, &param);
        param.sched_priority = sched_get_priority_max(policy);
        pthread_setschedparam(internal_transport_thread.native_handle(), policy, &param);
    }
    // erpc::bind_to_core(internal_transport_thread, numa_node, 10);
    while (!internal_transport_thread_ready);

    auto total_cores = std::thread::hardware_concurrency();

    ht_ct = totalThreads;
    nullApp->SetNumDurabilityReplicas(ht_ct);

    auto server_thread_func = [&](specpaxos::Configuration config, uint8_t numa_node, uint8_t thread_id)
    {
        FastTransport client_transport(config, ht_ct, 1, physical_port, numa_node, thread_id);

        auto durability_replica = specpaxos::vr::VRDurabilityReplica(config, index, !recover, &client_transport, batchSize, nullApp, internalConfig, &internal_transport, thread_id - 1);

        client_transport.Run();
    };
    std::vector<std::thread> durability_threads(ht_ct);
    uint8_t idx = 1;//i/4 + (i % 2) * 20;
    for (uint8_t i = 0; i < ht_ct; i++) {
        durability_threads[i] = std::thread(server_thread_func, clientConfig, numa_node, i + 1);
        if (idx % (total_cores / 2) == 0)
        {
            // Skip this core because of HT... background thread should occupy the whole physical core
            idx++;
        }
        erpc::bind_to_core(durability_threads[i], numa_node, idx);
        idx++;
    }

    for (auto &thread : durability_threads) thread.join();
    internal_transport_thread.join();
    // }
    nullApp->CloseDB();
}
