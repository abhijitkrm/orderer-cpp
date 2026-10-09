// orderer — an LMAX-style, multi-core order-matching engine around the
// matcher order book (C++20, header-only). Contract: orderer-spec/1.1.
#pragma once

#include "core.hpp"
#include "disruptor.hpp"
#include "egress.hpp"
#include "journal.hpp"
#include "pipeline.hpp"
#include "recover.hpp"
#include "routing.hpp"
#include "stats.hpp"
