// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * log.h:
 *   a replica's log of pending and committed operations
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

#include "common/log.h"
#include "common/request.pb.h"
#include "lib/assert.h"

#include <openssl/sha.h>

namespace specpaxos {

const string Log::EMPTY_HASH = string(SHA_DIGEST_LENGTH, '\0');

Log::Log(bool useHash, opnum_t start, string initialHash)
    : useHash(useHash)
{
    this->initialHash = initialHash;
    this->start = start;
    if (start == 1) {
        ASSERT(initialHash == EMPTY_HASH);
    }
}

void Log::Initialize(bool am_leader)
{
    Notice("Entry size %d", sizeof(LogEntry));
    this->initialHash = initialHash;
    this->start = start;
    if (start == 1) {
        ASSERT(initialHash == EMPTY_HASH);
    }

    // reserve enough space to reduce reallocation overhead
    // entries.resize(LOG_SIZE_PREALLOCATION);
    // entries_end = 0;
    // if (am_leader)
    // {
    //     std::for_each(std::execution::par_unseq,
    //         std::begin(entries),
    //         std::end(entries),
    //         [&](auto& entry)
    //     {
    //         auto req = message_allocator.Create<Request>();
    //         entry.req = std::move(req);
    //     });
    // }
}

LogEntry &
Log::Append(viewstamp_t vs, const Request &req, LogEntryState state, void *data, size_t data_len)
{
#ifndef LOG_DISABLE_CHECKS
    if (entries.empty()) {
        ASSERT(vs.opnum == start);
    } else {
        ASSERT(vs.opnum == LastOpnum()+1);
    }
#endif

    auto& entry = entries.InsertNoAllocation(entries_end);
    entry.viewstamp = vs;
    entry.state = state;
    entry.request() = req;

    if (entry.has_executed) {
        entry.has_executed = false;
    } else {
        Panic("Entry has not been executed!");
    }

    entries_end++;

    return entry;
}

void Log::Append(viewstamp_t vs, std::string_view op, uint64_t clientid, uint64_t clientreqid, LogEntryState state, void *data, size_t data_len)
{
#ifndef LOG_DISABLE_CHECKS
    if (entries.empty()) {
        ASSERT(vs.opnum == start);
    } else {
        ASSERT(vs.opnum == LastOpnum()+1);
    }
#endif

    auto& entry = entries.InsertNoAllocation(entries_end);
    entry.viewstamp = vs;
    entry.state = state;

    Request& request = entry.request();
	request.set_op(std::string(op));
	request.set_clientid(clientid);
	request.set_clientreqid(clientreqid);

    if (entry.has_executed) {
        entry.has_executed = false;
    } else {
        Panic("Entry has not been executed!");
    }

    entries_end++;
}

void Log::Append(viewstamp_t vs, Request* req, LogEntryState state, void *data, size_t data_len)
{
#ifndef LOG_DISABLE_CHECKS
    if (entries.empty()) {
        ASSERT(vs.opnum == start);
    } else {
        ASSERT(vs.opnum == LastOpnum()+1);
    }
#endif

    auto& entry = entries.InsertNoAllocation(entries_end);
    entry.viewstamp = vs;
    entry.state = state;
    entry.req = *req;

    if (entry.has_executed) {
        entry.has_executed = false;
    } else {
        Panic("Entry has not been executed!");
    }

    entries_end++;
}

LogEntry &
Log::Append(viewstamp_t vs,
            const Request &req,
            const std::set<viewstamp_t> &vss,
            LogEntryState state,
            void *data,
            size_t data_len)
{
    for (auto &v : vss) {
        vssMap[v] = vs.opnum;
    }
    return Append(vs, req, state, data, data_len);
}

// This really ought to be const
LogEntry *
Log::Find(opnum_t opnum)
{
#ifndef LOG_DISABLE_CHECKS
    if (entries.empty()) {
        return NULL;
    }

    if (opnum < start) {
        return NULL;
    }

    if (opnum-start > entries.size()-1) {
        return NULL;
    }

    LogEntry *entry = &entries.Get(opnum-start);
    ASSERT(entry->viewstamp.opnum == opnum);
#else
    LogEntry *entry = &entries.Get(opnum-start);
#endif
    return entry;
}

LogEntry *
Log::Find(viewstamp_t vs)
{
    Panic("Maps are disabled");
    return NULL;
}

LogEntry *
Log::Find(const std::pair<uint64_t, uint64_t> &reqid)
{
    Panic("Maps are disabled");
    return NULL;
}

bool
Log::SetStatus(opnum_t op, LogEntryState state)
{
    LogEntry *entry = Find(op);
    if (entry == NULL) {
        return false;
    }

    entry->state = state;
    return true;
}

bool
Log::SetRequest(opnum_t op, const Request &req)
{
    if (useHash) {
        Panic("Log::SetRequest on hashed log not supported.");
    }

    LogEntry *entry = Find(op);
    if (entry == NULL) {
        return false;
    }

    entry->request() = req;
    return true;
}

void
Log::RemoveAfter(opnum_t op)
{
#if PARANOID
    // We'd better not be removing any committed entries.
    for (opnum_t i = op; i <= LastOpnum(); i++) {
        ASSERT(Find(i)->state != LOG_STATE_COMMITTED);
    }
#endif

    if (op > LastOpnum()) {
        return;
    }

    Debug2("Removing log entries after " FMT_OPNUM, op);

    // ASSERT(op-start < entries.size());
    // entries.resize(op-start);

    // ASSERT(LastOpnum() == op-1);)
}

LogEntry *
Log::Last()
{
    if (entries.empty()) {
        return NULL;
    }

    return &entries.Get(entries_end - 1);
}

viewstamp_t
Log::LastViewstamp() const
{
    if (entries.empty()) {
        return viewstamp_t(0, start-1);
    } else {
        return entries.Get(entries_end - 1).viewstamp;
    }
}

opnum_t
Log::LastOpnum() const
{
    if (entries.empty()) {
        return start-1;
    } else {
        return entries.Get(entries_end - 1).viewstamp.opnum;
    }
}

opnum_t
Log::FirstOpnum() const
{
    // XXX Not really sure what's appropriate to return here if the
    // log is empty
    return start;
}

bool
Log::Empty() const
{
    return entries.empty();
}

const string &
Log::LastHash() const
{
    Panic("Unsupported");
    return "";
}

string
Log::ComputeHash(string lastHash, const LogEntry &entry)
{
    SHA_CTX ctx;
    unsigned char out[SHA_DIGEST_LENGTH];

    SHA1_Init(&ctx);

    SHA1_Update(&ctx, lastHash.c_str(), lastHash.size());
    //SHA1_Update(&ctx, &entry.viewstamp, sizeof(entry.viewstamp));
    uint64_t x[2];
    x[0] = entry.request().clientid();
    x[1] = entry.request().clientreqid();
    SHA1_Update(&ctx, x, sizeof(uint64_t)*2);
    // SHA1_Update(&ctx, entry.request.op().c_str(),
    //             entry.request.op().size());

    SHA1_Final(out, &ctx);

    return string((char *)out, SHA_DIGEST_LENGTH);
}

void Log::Trim(opnum_t op)
{
    LogEntry *entry = Find(op);
    if (entry == NULL) {
        Panic("Trim: opnum " FMT_OPNUM " not found", op);
    }
#ifdef LOG_TRIM_OVERWRITE
    for (auto i = entries_start; i < op; i++)
    {
        entry->has_executed = true;
        entries.DeleteWithoutDeallocating(i);
    }
    entries_start = std::min(op, entries_start);
#else
    if (!entry->has_executed)
    {
        entry->has_executed = true;
        entries.DeleteWithoutDeallocating(op - start);
        entries_start = op;
    }
    else
    {
        Panic("Trim: opnum " FMT_OPNUM " already executed", op);
    }
#endif
}

void Log::SetExecuted(opnum_t op, bool executed)
{
    LogEntry *entry = Find(op);
    if (entry == NULL) {
        Panic("SetExecuted: opnum " FMT_OPNUM " not found", op);
    }

    if (executed && entry->has_executed)
    {
        Panic("SetExecuted: opnum " FMT_OPNUM " already executed", op);
    }

    entry->has_executed = executed;
}

} // namespace specpaxos
