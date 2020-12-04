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

package worker

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
	tcpMetricsCount  = 2
	udpMetricsCount  = 11
	httpMetricsCount = 12
)

// Iperf results vary from protocol,required for proper parsing
const (
	zeroFormatTCP         = "0.0"
	fractionZeroFormatTCP = ".0"
	zeroFormatUDP         = "0.00"
	fractionZeroFormatUDP = ".00"
)

// Function to be applied for each metric for aggregation
var (
	iperfUDPFunction = []string{"", "", "", "Sum", "Sum", "Sum", "Sum", "Sum", "Avg", "Avg", "Min", "Max", "Avg", "Sum"}
	iperfTCPFunction = []string{"", "", "", "Sum", "Avg"}
)

// ParseResult parses the response received for each protocol type
func ParseResult(protocol string, result []string, testDuration string) ([]float64, error) {
	klog.Infof("Parsing response for %v", protocol)
	switch protocol {
	case ProtocolTCP:
		return parseIperfResponse(result, testDuration, ProtocolTCP, tcpMetricsCount, zeroFormatTCP, fractionZeroFormatTCP, iperfTCPFunction), nil
	case ProtocolUDP:
		return parseIperfResponse(result, testDuration, ProtocolUDP, udpMetricsCount, zeroFormatUDP, fractionZeroFormatUDP, iperfUDPFunction), nil
	case ProtocolHTTP:
		return parseSiegeResponse(result), nil
	default:
		return nil, errors.New("invalid protocol: " + protocol)
	}
}

func parseIperfResponse(result []string, testDuration string, protocol string, metricCount int, zeroFormat string, fractionZeroFormat string, operators []string) []float64 {
	klog.Info("Parsing iperf response")
	aggregatedResult := make([]float64, 0, metricCount)
	unitRegex := regexp.MustCompile(`%|\[\s+|\]\s+|KBytes\s+|KBytes/sec\s*|sec\s+|pps\s*|ms\s+|/|\(|\)\s+`)
	multiSpaceRegex := regexp.MustCompile(`\s+`)
	hyphenSpaceRegex := regexp.MustCompile(`\-\s+`)
	count := 0
	sessionID := make(map[string]bool)
	for _, op := range result {
		formattedString := hyphenSpaceRegex.ReplaceAllString(multiSpaceRegex.ReplaceAllString(unitRegex.ReplaceAllString(op, " "), " "), "-")
		if !strings.Contains(formattedString, zeroFormat+"-"+testDuration+fractionZeroFormat) {
			continue
		}
		split := strings.Split(formattedString, " ")
		//for bug in iperf tcp
		//if the record is for the complete duration of run
		if len(split) >= metricCount+1 && "SUM" != split[1] && split[2] == zeroFormat+"-"+testDuration+fractionZeroFormat {
			if _, ok := sessionID[split[1]]; ok {
				continue
			}
			for i, v := range split {
				if i == 1 {
					sessionID[v] = true
					continue
				}
				//first index and hte last is ""
				if i == 0 || i == 2 || i == metricCount+3 {
					continue
				}
				tmp, err := strconv.ParseFloat(v, 64)
				if err != nil {
					klog.Errorf("Conversion error %v", err)
				}
				if len(aggregatedResult) < metricCount {
					aggregatedResult = append(aggregatedResult, tmp)
				} else {
					switch operators[i] {
					case "Sum":
						aggregatedResult[i-3] = tmp + aggregatedResult[i-3]
					case "Avg":
						aggregatedResult[i-3] = (float64(count)*tmp + aggregatedResult[i-3]) / (float64(1 + count))
					case "Min":
						aggregatedResult[i-3] = math.Min(tmp, aggregatedResult[i-3])
					case "Max":
						aggregatedResult[i-3] = math.Max(tmp, aggregatedResult[i-3])
					}
				}
				count++
			}
		}
	}
	klog.Infof("Final output: %v", aggregatedResult)
	return aggregatedResult
}

func parseSiegeResponse(result []string) []float64 {
	canAppend := false
	aggregatedResult := make([]float64, 0, httpMetricsCount)
	multiSpaceRegex := regexp.MustCompile(`\s+`)
	for _, op := range result {
		if canAppend != true && strings.HasPrefix(op, "Transactions:") {
			canAppend = true
		}
		if canAppend == false {
			continue
		}
		formattedString := multiSpaceRegex.ReplaceAllString(op, " ")
		split := strings.Split(formattedString, ":")
		klog.Infof("Formatted: %v", formattedString)
		if len(split) > 1 {
			split := strings.Split(split[1], " ")
			if len(split) < 2 {
				continue
			}
			tmp, err := strconv.ParseFloat(split[1], 64)
			if err != nil {
				klog.Errorf("Error parsing: %v", err)
			}
			aggregatedResult = append(aggregatedResult, tmp)
		}

		if strings.HasPrefix(op, "Shortest transaction") {
			break
		}
	}
	klog.Infof("Final output: %v", aggregatedResult)
	return aggregatedResult
}

// GetValuesFromURL returns a map with values parsed from http request,for attributes specified in paramsToSearch
func GetValuesFromURL(request *http.Request, paramsToSearch []string) (map[string]string, error) {
	values := request.URL.Query()
	paramMap := make(map[string]string)
	for _, searchString := range paramsToSearch {
		val := values.Get(searchString)
		if val == "" {
			return nil, errors.New("missing required parameters")
		}
		paramMap[searchString] = val
	}
	return paramMap, nil
}
