#pragma once
#include <cstdint>
#include <vector>

#include <raftpp/detail/error.h>
#include <raftpp/detail/message.h>

namespace raft {
namespace tracker {

// Inflights limits the number of MsgAppend (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.

class Inflights
{
    // inflight describes an in-flight MsgAppend message.
    struct Inflight
    {
        Index index;  // the index of the last entry inside the message
        size_t bytes; // the total byte size of the entries in the message

        auto operator<=>(const Inflight&) const = default;
    };

public:
    // NewInflights sets up an Inflights that allows up to size inflight messages,
    // with the total byte size up to maxBytes. If maxBytes is 0 then there is no
    // byte size limit. The maxBytes limit is soft, i.e. we accept a single message
    // that brings it from size < maxBytes to size >= maxBytes.
    Inflights(size_t size, size_t maxBytes)
      : size_(size)
      , maxBytes_(maxBytes)
      , start_(0)
      , count_(0)
      , bytes_(0)
    {
    }

    // Add notifies the Inflights that a new message with the given index and byte
    // size is being dispatched. Full() must be called prior to Add() to verify that
    // there is room for one more message, and consecutive calls to Add() must
    // provide a monotonic sequence of indexes.
    void add(Index index, size_t bytes)
    {
        if (full()) {
            panic("cannot add into a Full inflights");
        }
        auto next = (start_ + count_) % size_;

        if (next >= buffer_.size()) {
            grow();
        }
        buffer_[next] = Inflight{ index, bytes };
        count_++;
        bytes_ += bytes;
    }

    // FreeLE frees the inflights smaller or equal to the given `to` flight.
    void freeLE(Index to)
    {
        if (count_ == 0 || to < buffer_[start_].index) {
            // out of the left side of the window
            return;
        }

        size_t n = 0;
        size_t bytes = 0;
        for (; n < count_; n++) {
            if (to < buffer_[start_].index) { // found the first large inflight
                break;
            }
            bytes += buffer_[start_].bytes;

            // increase index and maybe rotate
            start_ = (start_ + 1) % size_;
        }

        // free i inflights and set new start index
        count_ -= n;
        bytes_ -= bytes;
        if (count_ == 0) {
            // inflights is empty, reset the start index so that we don't grow the
            // buffer unnecessarily.
            start_ = 0;
        }
    }

    // Full returns true if no more messages can be sent at the moment.
    bool full() const { return count_ == size_ || (maxBytes_ != 0 && bytes_ >= maxBytes_); }

    // Count returns the number of inflight messages.
    size_t count() const { return count_; }

    // reset frees all inflights.
    void reset()
    {
        start_ = 0;
        count_ = 0;
        bytes_ = 0;
    }

    // test
    bool isEqual(size_t start, size_t count, size_t bytes, size_t size, const std::vector<Inflight>& inflights) const
    {
        auto eq = [&](const std::vector<Inflight>& in) {
            if (count_ != in.size()) {
                return false;
            }
            for (size_t i = 0; i < count_; i++) {
                if (buffer_[start_ + i % size_] != in[i]) {
                    return false;
                }
            }
            return true;
        };

        return (start_ == start) && (count_ == count) && (bytes_ == bytes) && (size_ == size) && eq(inflights);
    }

private:
    // grow the inflight buffer by doubling up to inflights.size. We grow on demand
    // instead of preallocating to inflights.size to handle systems which have
    // thousands of Raft groups per process.
    void grow()
    {
        auto newSize = buffer_.size() * 2;
        if (newSize == 0) {
            newSize = 1;
        } else if (newSize > size_) {
            newSize = size_;
        }
        // buffer_.reserve(newSize);
        buffer_.resize(newSize, { 0, 0 });
    }

    // the starting index in the buffer
    size_t start_;

    size_t count_; // number of inflight messages in the buffer
    size_t bytes_; // number of inflight bytes

    const size_t size_;     // the max number of inflight messages
    const size_t maxBytes_; // the max total byte size of inflight messages

    // buffer is a ring buffer containing info about all in-flight messages.
    std::vector<Inflight> buffer_;
};

} // namespace tracker
} // namespace raft