/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package network

import (
	measurementutil "k8s.io/perf-tests/clusterloader2/pkg/measurement/util"
)

const (
	ProtocolTCP  = "TCP"
	ProtocolUDP  = "UDP"
	ProtocolHTTP = "HTTP"
)

//TCP result array Index mapping
const (
	TCPTransfer = iota
	TCPBW
)

//UDP result array Index mapping
const (
	UDPTransfer = iota
	UDPBW
	UDPJitter
	UDPLostPkt
	UDPTotalPkt
	UDPLatPer
	UDPLatAvg
	UDPLatMin
	UDPLatMax
	UDPLatStdD
	UDPPps
)

//HTTP result array Index mapping
const (
	HTTPTxs = iota
	HTTPAvl
	HTTPTimeElps
	HTTPDataTrsfd
	HTTPRespTime
	HTTPTxRate
	HTTPThroughput
	HTTPConcurrency
	HTTPTxSuccesful
	HTTPFailedTxs
	HTTPLongestTx
	HTTPShortestTx
)

const (
	RatioSeparator   = ":"
	netperfNamespace = "netperf"
)

//WorkerPodData represents details of Pods running on worker node
type workerPodData struct {
	podName    string
	workerNode string
	podIp      string
	clusterIP  string
}

type WorkerResponse struct {
	PodName    string
	WorkerNode string
	Error      string
}

//UniquePodPair represents src-dest worker pod pair.
type uniquePodPair struct {
	SrcPodName    string
	SrcPodIp      string
	DestPodName   string
	DestPodIp     string
	IsLastPodPair bool `default: false`
}

type MetricRequest struct {
}

type MetricResponse struct {
	Result          []float64
	WorkerStartTime string
	Error           string
}

type NetworkPerfResp struct {
	Client_Server_Ratio string
	Protocol            string
	Service             string
	DataItems           []measurementutil.DataItem
}

//Client-To-Server Pod ratio indicator
const (
	OneToOne   = "1:1"
	ManyToOne  = "N:1"
	ManyToMany = "N:M"
	Invalid    = "Invalid"
)

const (
	Server = iota
	Client
)

const (
	Percentile05 = 0.05
	Percentile50 = 0.50
	Percentile95 = 0.95
)

const (
	Perc05 = "Perc05"
	Perc50 = "Perc50"
	Perc95 = "Perc95"
	Min    = "min"
	Max    = "max"
	Avg    = "avg"
	value  = "value"
)

var httpPathMap = map[string]map[int]string{
	ProtocolTCP:  map[int]string{Server: "startTCPServer", Client: "startTCPClient"},
	ProtocolUDP:  map[int]string{Server: "startUDPServer", Client: "startUDPClient"},
	ProtocolHTTP: map[int]string{Server: "startHTTPServer", Client: "startHTTPClient"},
}

const (
	Throughput   = "Throughput"
	Latency      = "Latency"
	Jitter       = "Jitter"
	PPS          = "Packet_Per_Second"
	ResponseTime = "Response_Time"
)

var metricUnitMap = map[string]string{
	Throughput:   "kbytes/sec",
	Latency:      "ms",
	Jitter:       "ms",
	PPS:          "pps",
	ResponseTime: "seconds",
}

// DataItem is the data point.
type DataItem struct {
	Data   map[string]float64 `json:"data"`
	Unit   string             `json:"unit"`
	Labels map[string]string  `json:"labels,omitempty"`
}
