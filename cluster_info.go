// Copyright 2022, SuccessfulMatch.com All rights reserved.
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/12

// The redis cluster info.

package redis

import (
    "strconv"
    "strings"
)

type clusterInfo struct {
    Cluster_state                        string
    Cluster_slots_assigned               uint64
    Cluster_slots_ok                     uint64
    Cluster_slots_pfail                  uint64
    Cluster_slots_fail                   uint64
    Cluster_known_nodes                  uint64
    Cluster_size                         uint64
    Cluster_my_epoch                     uint64
    Cluster_stats_messages_ping_sent     uint64
    Cluster_stats_messages_pong_received uint64
    Cluster_stats_messages_sent          uint64
    Cluster_stats_messages_received      uint64
    Cluster_stats_messages_ping_received uint64
    Cluster_stats_messages_pong_sent     uint64
    Cluster_stats_messages_meet_received uint64
}

var (
    oneClusterInfo      *clusterInfo
)

func NewClusterInfo(info string) *clusterInfo {
    if oneClusterInfo == nil {
        oneClusterInfo    = &clusterInfo{}
    }
    oneClusterInfo.ParseAndSetInfo(info)

    return oneClusterInfo
}

func (this *clusterInfo) str2uint64(str string) uint64 {
    var res uint64 = 0
    if v, err := strconv.ParseUint(str, 10, 64); err == nil {
        res = v
    }
    return res
}

func (this *clusterInfo) ParseAndSetInfo(info string) {
    comps   := strings.Split(info, "\r\n")

    for _, item := range comps {
        subComps := strings.Split(item, ":")
        if len(subComps) != 2 {
            continue
        }

        switch subComps[0] {
        case "cluster_state":
            this.Cluster_state          = subComps[1]
        case "cluster_slots_assigned":
            this.Cluster_slots_assigned = this.str2uint64(subComps[1])
        case "cluster_slots_ok":
            this.Cluster_slots_ok       = this.str2uint64(subComps[1])
        case "cluster_slots_pfail":
            this.Cluster_slots_pfail    = this.str2uint64(subComps[1])
        case "cluster_slots_fail":
            this.Cluster_slots_fail     = this.str2uint64(subComps[1])
        case "cluster_known_nodes":
            this.Cluster_known_nodes    = this.str2uint64(subComps[1])
        case "cluster_size":
            this.Cluster_size           = this.str2uint64(subComps[1])
        case "cluster_my_epoch":
            this.Cluster_my_epoch       = this.str2uint64(subComps[1])
        case "cluster_stats_messages_ping_sent":
            this.Cluster_stats_messages_ping_sent       = this.str2uint64(subComps[1])
        case "cluster_stats_messages_pong_received":
            this.Cluster_stats_messages_pong_received   = this.str2uint64(subComps[1])
        case "cluster_stats_messages_sent":
            this.Cluster_stats_messages_sent            = this.str2uint64(subComps[1])
        case "cluster_stats_messages_received":
            this.Cluster_stats_messages_received        = this.str2uint64(subComps[1])
        case "cluster_stats_messages_ping_received":
            this.Cluster_stats_messages_ping_received   = this.str2uint64(subComps[1])
        case "cluster_stats_messages_pong_sent":
            this.Cluster_stats_messages_pong_sent       = this.str2uint64(subComps[1])
        case "cluster_stats_messages_meet_received":
            this.Cluster_stats_messages_meet_received   = this.str2uint64(subComps[1])
        }
    }
}
