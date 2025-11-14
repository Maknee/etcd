// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * transport.cc:
 *   message-passing network interface; common definitions
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

#include "lib/assert.h"
#include "lib/transport.h"

TransportReceiver::~TransportReceiver()
{
    delete this->myAddress;
}

void
TransportReceiver::SetAddress(const TransportAddress *addr)
{
    this->myAddress = addr;
}

const TransportAddress &
TransportReceiver::GetAddress()
{
    return *(this->myAddress);
}

ManualCallBack::ManualCallBack(Transport *transport, timer_callback_t cb)
		: transport(transport), cb(cb)	{
	Id = -1;
}

ManualCallBack::~ManualCallBack()
{

}
void
ManualCallBack::Start()
    {
        Id=  transport->ManualEvent([this]() {
            Id = -1;
            cb();
        });
    }

Timeout::Timeout(Transport *transport, uint64_t ms, timer_callback_t cb, uint64_t us)
    : transport(transport), ms(ms), cb(cb), us(us)
{
    timerId = -1;
}

Timeout::~Timeout()
{
    Stop();
}

void
Timeout::SetTimeout(uint64_t ms)
{
    ASSERT(!Active());
    this->ms = ms;
}

uint64_t
Timeout::Start()
{
    return this->Reset();
}


uint64_t
Timeout::Reset()
{
    Stop();

    if (ms != 0)
    {
        timerId = transport->Timer(ms, [this]() {
            timerId = -1;
            Reset();
            cb();
        });
    }
    else
    {
        timerId = transport->TimerUS(us, [this]() {
            timerId = -1;
            Reset();
            cb();
        });
    }

    return ms;
}

void
Timeout::Stop()
{
    if (timerId >= 0) {
        transport->CancelTimer(timerId);
        timerId = -1;
    }
}

bool
Timeout::Active() const
{
    return (timerId >= 0);
}

