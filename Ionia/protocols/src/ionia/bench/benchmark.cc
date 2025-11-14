// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * benchmark.cpp:
 *   simple replication benchmark client
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

#include "bench/benchmark.h"
#include "common/client.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "lib/timeval.h"

#include <sys/time.h>
#include <string>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <iomanip>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/mman.h>

#define OPCODE_SIZE 1
#define KEY_SIZE 24
#define VAL_SIZE 100

namespace specpaxos {

DEFINE_LATENCY(op);

BenchmarkClient::BenchmarkClient(Client &client, Transport &transport,
                                 int numRequests_, uint64_t delay,
                                 int warmupSec,
				 int tputInterval,
                 string traceFile,
                 int experimentDuration,
                 bool logLatency,
                 string latencyFilename)
    : tputInterval(tputInterval), client(client),
    transport(transport), numRequests(numRequests_),
    delay(delay), warmupSec(warmupSec), traceFile(traceFile),
    experimentDuration(experimentDuration), logLatency(logLatency), latencyFilename(latencyFilename)
{
    if (delay != 0) {
        Notice("Delay between requests: %ld ms", delay);
    }
    started = false;
    done = false;
    cooldownDone = false;
    _Latency_Init(&latency, "op");

    Notice("Using tracefile: %s", traceFile.c_str());
    int traceFd = open(traceFile.c_str(), O_RDONLY);
    assert(traceFd);
    struct stat stat_buf;
    int rc = 0;
    assert(!(rc = stat(traceFile.c_str(), &stat_buf)));
    void* vaddr =  mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED , traceFd, 0);
    std::size_t n = 0;
    {
        size_t off = 0;
        char* curr = (char*) vaddr;
        while(off < (uint32_t) stat_buf.st_size) {
            char* op_ptr = curr;
            curr += OPCODE_SIZE + 1; 
            off += OPCODE_SIZE + 1;

            char* key_ptr = curr;
            curr += KEY_SIZE + 1;
            off += KEY_SIZE + 1;
            n++;
        }
    }

    size_t off = 0;
    char* curr = (char*) vaddr;
    numRequests = std::min(numRequests, (int)n);
    opcodes.reserve(numRequests);
    messages.reserve(numRequests);

    std::size_t i = 0;
    while(off < (uint32_t) stat_buf.st_size) {
        if (i == numRequests)
        {
            break;
        }
        char* op_ptr = curr;
        curr += OPCODE_SIZE + 1; 
        off += OPCODE_SIZE + 1;

        char* key_ptr = curr;
        curr += KEY_SIZE + 1;
        off += KEY_SIZE + 1;

        string op = std::string(op_ptr, OPCODE_SIZE);
        string key = std::string(key_ptr, KEY_SIZE);
        assert(op.length() == OPCODE_SIZE);
        assert(key.length() == KEY_SIZE);

        std::ostringstream msg;

        msg << op << key;
        bool isRead = op[0] == 'r' || op[0] == 'R';
        bool isUpdate = op[0] == 'u' || op[0] == 'U';
        bool isNonNilext = op[0] == 'e' || op[0] == 'E';

        if(!isRead) {
            if (isUpdate || isNonNilext)
                msg << string(VAL_SIZE, 'x');
            else
                msg << string(VAL_SIZE, 'v');
        }
        auto str = msg.str();

        //Latency_Start(&latency);
        opcodes.push_back(op[0]);
        messages.emplace_back(str);

        // load 100 more requests from the file 
        // sometimes the benchmark sends slightly more requests than n...
        if(messages.size() >= (uint32_t) numRequests + 100) {
            break;
        }

        i++;
        // Notice("op: %s key:%s", operations.back().first.c_str(), operations.back().second.c_str());
    }
    munmap(vaddr, stat_buf.st_size);
    close(traceFd);

    if (logLatency)
    {
        latencies.reserve(numRequests);
    }
    else
    {
        // populate enough
        for (auto i = 0; i < 1000; i++)
        {
            latencies.push_back(1);
        }
    }
    opcodes.reserve(numRequests);

    Notice("Loaded %lu operations (numrequests = %d) from tracefile: %s",
     messages.size(), numRequests, traceFile.c_str());
}

void
BenchmarkClient::Start()
{
    n = 0;
    transport.Timer(warmupSec * 1000,
                    std::bind(&BenchmarkClient::WarmupDone,
                               this));

    if (tputInterval > 0) {
	msSinceStart = 0;
	opLastInterval = n;
	transport.Timer(tputInterval, std::bind(&BenchmarkClient::TimeInterval,
						this));
    }
    expStartTime = std::chrono::high_resolution_clock::now();
    SendNext();
}

void
BenchmarkClient::TimeInterval()
{
    if (done) {
	return;
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);
    msSinceStart += tputInterval;
    Notice("Completed %d requests at %lu ms", n-opLastInterval, (((tv.tv_sec*1000000+tv.tv_usec)/1000)/10)*10);
    opLastInterval = n;
    transport.Timer(tputInterval, std::bind(&BenchmarkClient::TimeInterval,
					    this));
}

void
BenchmarkClient::WarmupDone()
{
    started = true;
    Notice("Completed warmup period of %d seconds with %d requests",
           warmupSec, n);
    gettimeofday(&startTime, NULL);
    n = 0;
}

void
BenchmarkClient::CooldownDone()
{

    char buf[1024];
    cooldownDone = true;
    Notice("Finished cooldown period.");
    std::vector<uint64_t> sorted = latencies;
    std::sort(sorted.begin(), sorted.end());

    uint64_t ns = sorted[sorted.size()/2];
    LatencyFmtNS(ns, buf);
    Notice("Median latency is %ld ns (%s)", ns, buf);

    ns = 0;
    for (auto latency : sorted) {
        ns += latency;
    }
    ns = ns / sorted.size();
    LatencyFmtNS(ns, buf);
    Notice("Average latency is %ld ns (%s)", ns, buf);

    ns = sorted[sorted.size()*90/100];
    LatencyFmtNS(ns, buf);
    Notice("90th percentile latency is %ld ns (%s)", ns, buf);

    ns = sorted[sorted.size()*95/100];
    LatencyFmtNS(ns, buf);
    Notice("95th percentile latency is %ld ns (%s)", ns, buf);

    ns = sorted[sorted.size()*99/100];
    LatencyFmtNS(ns, buf);
    Notice("99th percentile latency is %ld ns (%s)", ns, buf);
}

void
BenchmarkClient::SendNext()
{
    if(n >= numRequests){
        return;
    }

    /*
    std::ostringstream msg;

    msg << operations[n].first << operations[n].second;
    auto str = msg.str();
    bool isRead = str.c_str()[0] == 'r' || str.c_str()[0] == 'R';
    bool isUpdate = str.c_str()[0] == 'u' || str.c_str()[0] == 'U';
    bool isNonNilext = str.c_str()[0] == 'e' || str.c_str()[0] == 'E';

    if(!isRead) {
    	if (isUpdate || isNonNilext)
    		msg << string(VAL_SIZE, 'x');
    	else
    		msg << string(VAL_SIZE, 'v');
    }

    //Latency_Start(&latency);
    opcodes.push_back(str.c_str()[0]);
    */

    Latency_Start(&latency);
    //latency_time = std::chrono::high_resolution_clock::now();
    const auto& str = messages[n]; 
    client.Invoke(str, std::bind(&BenchmarkClient::OnReply,
                                    this,
                                    std::placeholders::_1,
                                    std::placeholders::_2));    
}

void
BenchmarkClient::OnReply(const string &request, const string &reply)
{
    if (cooldownDone) {
        return;
    }

    n++;
    if ((started) && (!done) && (n != 0)) {
    	uint64_t ns = Latency_End(&latency);
        if (logLatency)
        {
        	latencies.push_back(ns);
        }
    	if (n >= numRequests) {
    	    Finish();
    	}

        auto current_time = std::chrono::high_resolution_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(current_time 
            - expStartTime).count() >= experimentDuration) {
			Notice("Experiment duration elasped. Exiting.");
            Finish();
        }
    }
    
    if (delay == 0) {
       SendNext();
    } else {
        uint64_t rdelay = rand() % delay*2;
        transport.Timer(rdelay,
                        std::bind(&BenchmarkClient::SendNext, this));
    }
}

static std::mutex finishLock;

void
BenchmarkClient::Finish()
{
    std::lock_guard<std::mutex> l(finishLock);
    gettimeofday(&endTime, NULL);

    struct timeval diff = timeval_sub(endTime, startTime);

    Notice("Completed %d requests in " FMT_TIMEVAL_DIFF " seconds",
           n, VA_TIMEVAL_DIFF(diff));
    std::cout << std::endl;
    done = true;

    transport.Timer(warmupSec * 1000,
                    std::bind(&BenchmarkClient::CooldownDone,
                              this));


    if (latencyFilename.size() > 0) {
        Latency_FlushTo(latencyFilename.c_str());
    }
}


} // namespace specpaxos
