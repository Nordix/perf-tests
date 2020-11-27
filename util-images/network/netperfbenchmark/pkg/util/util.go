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

// Package util contains utility methods for worker
package util

import (
	"errors"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/klog"
)

// Protocols supported
const (
	ProtocolTCP  = "TCP"
	ProtocolUDP  = "UDP"
	ProtocolHTTP = "HTTP"
)

// Number of metrics for each protocol
const (
	tcpMtrCnt   = 2
	udpMtrCnt   = 11
	httpMetrCnt = 12
)

// Iperf results vary from protocol,required for proper parsing
const (
	zeroFmtTCP         = "0.0"
	fractionZeroFmtTCP = ".0"
	zeroFmtUDP         = "0.00"
	fractionZeroFmtUDP = ".00"
)

// Function to be applied for each metric for aggregation
var (
	iperfUDPFn = []string{"", "", "", "Sum", "Sum", "Sum", "Sum", "Sum", "Avg", "Avg", "Min", "Max", "Avg", "Sum"}
	iperfTCPFn = []string{"", "", "", "Sum", "Avg"}
)

// ParseResult parses the response received for each protocol type
func ParseResult(protType string, result []string, testDuration string) ([]float64, error) {
	klog.Info("Parsing response for ", protType)
	switch protType {
	case ProtocolTCP:
		return parseIperfResponse(result, testDuration, ProtocolTCP, tcpMtrCnt, zeroFmtTCP, fractionZeroFmtTCP, iperfTCPFn), nil
	case ProtocolUDP:
		return parseIperfResponse(result, testDuration, ProtocolUDP, udpMtrCnt, zeroFmtUDP, fractionZeroFmtUDP, iperfUDPFn), nil
	case ProtocolHTTP:
		return parseSiegeResponse(result), nil
	default:
		return nil, errors.New("invalid protocol:" + protType)
	}
}

func parseIperfResponse(result []string, testDuration string, protocol string, metricCnt int, zeroFmt string, fractionZeroFmt string, operType []string) []float64 {
	klog.Info("In parseIperf")
	sumResult := make([]float64, 0, metricCnt)
	unitReg := regexp.MustCompile(`%|\[\s+|\]\s+|KBytes\s+|KBytes/sec\s*|sec\s+|pps\s*|ms\s+|/|\(|\)\s+`)
	mulSpaceReg := regexp.MustCompile(`\s+`)
	hypSpcReg := regexp.MustCompile(`\-\s+`)
	cnt := 0
	sessionID := make(map[string]bool)
	for _, op := range result {
		klog.Info(op)
		frmtString := hypSpcReg.ReplaceAllString(mulSpaceReg.ReplaceAllString(unitReg.ReplaceAllString(op, " "), " "), "-")
		if !strings.Contains(frmtString, zeroFmt+"-"+testDuration+fractionZeroFmt) {
			continue
		}
		klog.Info("Trim info:", frmtString)
		split := strings.Split(frmtString, " ")
		//for bug in iperf tcp
		//if the record is for the complete duration of run
		if len(split) >= metricCnt+1 && "SUM" != split[1] && split[2] == zeroFmt+"-"+testDuration+fractionZeroFmt {
			if _, ok := sessionID[split[1]]; ok {
				continue
			}
			for i, v := range split {
				klog.Info("Split", i, ":", v)
				if i == 1 {
					sessionID[v] = true
					continue
				}
				//first index and hte last is ""
				if i == 0 || i == 2 || i == metricCnt+3 {
					continue
				}
				tmp, err := strconv.ParseFloat(v, 64)
				if err != nil {
					klog.Error("Conversion error", err)
				}
				if len(sumResult) < metricCnt {
					sumResult = append(sumResult, tmp)
				} else {
					switch operType[i] {
					case "Sum":
						sumResult[i-3] = tmp + sumResult[i-3]
					case "Avg":
						sumResult[i-3] = (float64(cnt)*tmp + sumResult[i-3]) / (float64(1 + cnt))
					case "Min":
						sumResult[i-3] = math.Min(tmp, sumResult[i-3])
					case "Max":
						sumResult[i-3] = math.Max(tmp, sumResult[i-3])
					}
				}
				cnt++
			}
		}
	}
	klog.Info("Final output:", sumResult)
	return sumResult
}

func parseSiegeResponse(result []string) []float64 {
	canAppend := false
	sumResult := make([]float64, 0, httpMetrCnt)
	mulSpaceReg := regexp.MustCompile(`\s+`)
	for _, op := range result {
		if canAppend != true && strings.HasPrefix(op, "Transactions:") {
			canAppend = true
		}
		if canAppend == false {
			continue
		}
		fmtStr := mulSpaceReg.ReplaceAllString(op, " ")
		split := strings.Split(fmtStr, ":")
		klog.Info("Formatted:", fmtStr)
		if len(split) > 1 {
			split := strings.Split(split[1], " ")
			if len(split) < 2 {
				continue
			}
			tmp, err := strconv.ParseFloat(split[1], 64)
			if err != nil {
				klog.Error("Error parsing:", err)
			}
			sumResult = append(sumResult, tmp)
		}

		if strings.HasPrefix(op, "Shortest transaction") {
			break
		}
	}
	klog.Info("Final output:", sumResult)
	return sumResult
}

// GetValuesFromURL returns a map with values parsed from http request,for attributes specified in paramsToSearch
func GetValuesFromURL(req *http.Request, paramsToSearch []string) (map[string]string, error) {
	values := req.URL.Query()
	paramMap := make(map[string]string)
	for _, s := range paramsToSearch {
		val := values.Get(s)
		if val == "" {
			return nil, errors.New("missing required parameters")
		}
		paramMap[s] = val
	}
	return paramMap, nil
}
