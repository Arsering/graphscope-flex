#!/usr/bin/env bpftrace

kprobe:filemap_fault / comm == "rt_server" /
{
    @entry_ts[tid] = nsecs;
}

kretprobe:filemap_fault / @entry_ts[tid] /
{
    $entry_time = @entry_ts[tid];
    $duration_us = (nsecs - $entry_time);

    @total_duration[tid] = @total_duration[tid] + $duration_us;

    delete(@entry_ts[tid]);
}

END
{
    clear(@entry_ts);
}