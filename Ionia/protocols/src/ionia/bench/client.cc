// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * client.cpp:
 *   test instantiation of a client application
 *
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

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "lib/fasttransport.h"

#include "bench/benchmark.h"
#include "common/client.h"
#include "lib/configuration.h"
#include "vr/client.h"

#include <unistd.h>
#include <stdlib.h>
#include <fstream>

static void
Usage(const char *progName)
{
        fprintf(stderr, "usage: %s [-n requests] [-t threads] [-w warmup-secs] [-l latency-file] [-q dscp] [-d delay-ms] -c conf-file -m vr\n",
                progName);
        exit(1);
}

void
PrintReply(const string &request, const string &reply)
{
    Notice("Request succeeded; got response %s", reply.c_str());
}

int main(int argc, char **argv)
{
    const char *configPath = NULL;
    const char *consensusConfigPath = NULL;
    int clientId = 1;
    int numRequests = 1000;
    int warmupSec = 0;
    int dscp = 0;
    uint64_t delay = 0;
    uint64_t experimentDuration = 60; // 60 second experiments by default
    int tputInterval = 0;
    int loadStartIndex = 0;
    int totalThreads = 0;
    bool dontLogLatency = false;

    enum
    {
        PROTO_UNKNOWN,
        PROTO_VR
    } proto = PROTO_UNKNOWN;

    string latencyFile;
    string latencyRawFile;
    string traceFilePrefix;

    // Parse arguments
    int opt;
    while ((opt = getopt(argc, argv, "c:d:e:q:k:l:m:n:t:w:i:z:j:p:s:")) != -1) {
        switch (opt) {
        case 'c':
            configPath = optarg;
            break;

        case 's':
            consensusConfigPath = optarg;
            break;

        case 'd':
        {
            char *strtolPtr;
            delay = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -d requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'e':
        {
            char *strtolPtr;
            experimentDuration = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -e requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

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

        case 'k':
            traceFilePrefix = string(optarg);
            break;

        case 'l':
            latencyFile = string(optarg);
            break;

        case 'm':
            if (strcasecmp(optarg, "vr") == 0) {
                proto = PROTO_VR;
            } else {
                fprintf(stderr, "unknown mode '%s'\n", optarg);
                Usage(argv[0]);
            }
            break;

        case 'n':
        {
            char *strtolPtr;
            numRequests = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (numRequests <= 0))
            {
                fprintf(stderr,
                        "option -n requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 't':
        {
            char *strtolPtr;
            clientId = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (clientId < 0))
            {
                fprintf(stderr,
                        "option -t requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'w':
        {
            char *strtolPtr;
            warmupSec = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (numRequests <= 0))
            {
                fprintf(stderr,
                        "option -w requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

	case 'i':
        {
            char *strtolPtr;
            tputInterval = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -d requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }
    	case 'z':
        {
            char *strtolPtr;
            loadStartIndex = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -z requires a numeric arg\n");
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
                        "option -j requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }
    	case 'p':
        {
            char *strtolPtr;
            dontLogLatency = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -p requires a numeric arg\n");
                Usage(argv[0]);
            }
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
    if (!consensusConfigPath) {
        fprintf(stderr, "option -s (consensusConfigPath) is required\n");
        Usage(argv[0]);
    }
    if (proto == PROTO_UNKNOWN) {
        fprintf(stderr, "option -m is required\n");
        Usage(argv[0]);
    }

    // Load configuration
    std::ifstream configStream(configPath);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                configPath);
        Usage(argv[0]);
    }
    specpaxos::Configuration config(configStream);

    // Load consensus configuration
    std::ifstream consensusConfigStream(consensusConfigPath);
    if (consensusConfigStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                configPath);
        Usage(argv[0]);
    }
    specpaxos::Configuration consensusConfig(consensusConfigStream);

    std::vector<specpaxos::Client *> clients;
    std::vector<specpaxos::BenchmarkClient *> benchClients;

    std::vector<FastTransport*> transports;
    std::vector<std::thread> benchmark_client_threads;
    std::mutex sync_m;

    const uint8_t numa_node = 0;
    std::atomic<bool> start_running_transport = false;
    std::atomic<int> threads_finished_setup = 0;

    auto physical_port = 0x0;
    auto client_thread_func = [&](specpaxos::Configuration config, uint8_t numa_node, uint8_t thread_id, size_t total_threads)
    {
        // Some hardware has limit of x of queues for rpc (16 for xl170)
        const size_t num_socket_cores = erpc::get_lcores_for_numa_node(numa_node).size();
        auto ht_ct = std::min(num_socket_cores, static_cast<std::size_t>(numRequests));

        FastTransport transport(config, ht_ct, 1, physical_port, numa_node, thread_id);

        {
            specpaxos::Client *client;

            auto client_id = thread_id + (clientId * total_threads);
            auto traceFileNum = thread_id;
            if (!loadStartIndex)
            {
                traceFileNum += ((clientId % total_threads) * total_threads);
            }
            else
            {
                traceFileNum += loadStartIndex;
            }
            switch (proto) {
            
            case PROTO_VR:
                client = new specpaxos::vr::VRClient(config, &transport, client_id);
                break;

            default:
                NOT_REACHABLE();
            }

            // clientId is the client machine ID
            // each machine will have 16 clients underneath
            
            // for machine 0, traceFileNums will be between 1 and 16 
            // for machine 1, traceFileNums will be between 17 and 32
            // and so on 
    	    string traceFile = traceFilePrefix + std::to_string(traceFileNum);

            specpaxos::BenchmarkClient *bench =
                    new specpaxos::BenchmarkClient(*client, transport,
                                                numRequests, delay,
                                                warmupSec, tputInterval, traceFile, (int) experimentDuration, !dontLogLatency);

            //transport.Timer(0, [=]() {bench->Start(); });

            sync_m.lock();
            clients.push_back(client);
            benchClients.push_back(bench);
        }

        transports.push_back(&transport);

        Timeout checkTimeout(&transport, 100, [&]() {
            for (auto x : benchClients) {
                if (!x->cooldownDone) {
                    return;
                }
            }

            for(auto& transport : transports)
            {
                transport->Stop();
            }

            Notice("All clients done.");

            Latency_t sum;
            _Latency_Init(&sum, "total");
            for (unsigned int i = 0; i < benchClients.size(); i++) {
                Latency_Sum(&sum, &benchClients[i]->latency);
            }
            Latency_Dump(&sum);
            if (latencyFile.size() > 0) {
                Latency_FlushTo(latencyFile.c_str());
            }

            latencyRawFile = latencyFile+".raw";
            std::ofstream rawFile(latencyRawFile.c_str());        

            for (auto x : benchClients) {
                int index = 0;
                for (const auto &e : x->latencies) {
                    rawFile << x->opcodes[index++] << e << "\n";
                }
            }
            rawFile.close();
            exit(0);
        });
        static bool start = false;
        if (!start)
        {
            start = true;
            checkTimeout.Start();
        }

        sync_m.unlock();
        threads_finished_setup++;

        while(!start_running_transport) {}
        transport.Run();
    };

    // Just use the first config as reference for number of threads per server
    uint8_t coreBoundIdx = 0;
    for (auto i = 0; i < 1; i++) {
        auto replica_config = config.replica(0, i);

        for (auto j = 0; j < totalThreads; j++) {
            auto benchmark_client_thread = std::thread(client_thread_func, config, numa_node, j + 1, totalThreads);
            benchmark_client_threads.emplace_back(std::move(benchmark_client_thread)); 
            erpc::bind_to_core(benchmark_client_threads.back(), numa_node, coreBoundIdx);
            coreBoundIdx++;
        }

        while (threads_finished_setup != totalThreads) {
            continue;
        }
        Notice("%d != %d", benchClients.size(), totalThreads);
        for (auto i = 0; i < benchClients.size(); i++) {
            auto* bench = benchClients[i];
            auto* transport = transports[i];
            transport->Timer(0, [=]() {bench->Start(); });
            Notice("Running client... %d", i);
        }
        start_running_transport = true;
    }

    for (auto &thread : benchmark_client_threads) thread.join();
}
