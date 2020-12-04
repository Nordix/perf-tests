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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/client-go/kubernetes"
	measurementutil "k8s.io/perf-tests/clusterloader2/pkg/measurement/util"

	"k8s.io/klog"
)

type controller struct {
	// podRatio is client-server pod ratio (1:1, N:M , N:1)
	podRatio     string
	testDuration int
	protocol     string
	podRatioType string
	// workerPodList stores list of podData for every worker node.
	workerPodList map[string][]workerPodData
	// metricVal stores MetricResponse received from worker node.
	metricVal map[string]MetricResponse
	// uniqPodPairList contains unique worker pod pairs located on different nodes.
	uniqPodPairList []uniquePodPair
	// metricRespPendingList contains pods from whom response is still pending.
	metricRespPendingList []uniquePodPair
	k8sClient             kubernetes.Interface

	// futureTimeStampForTestExec is used to send futureTimeStamp to all pods to start
	// sending the traffic at the same time.
	futureTimeStampForTestExec int64
	networkPerfRespForDisp     NetworkPerfResp

	podPairCh chan uniquePodPair
}

const (
	// initialDelay in seconds after which pod starts sending traffic.
	// Delay is used to synchronize all client pods to send traffic at same time.
	initialDelayForTCExec = 5
	// WorkerListerPort is the port on which worker pod listens for http request.
	WorkerListenPort = "5003"
)

func (ctrl *controller) startController() {
	ctrl.workerPodList = make(map[string][]workerPodData)
	ctrl.metricVal = make(map[string]MetricResponse)
	clientPodNum, _, podRatioType := deriveClientServerPodNum(ctrl.podRatio)
	ctrl.podRatioType = podRatioType
	ctrl.uniqPodPairList = make([]uniquePodPair, 0, clientPodNum)
	ctrl.metricRespPendingList = make([]uniquePodPair, 0, clientPodNum)
	ctrl.podPairCh = make(chan uniquePodPair)
}

func (ctrl *controller) populateWorkerPodList(data *workerPodData) {
	if podData, ok := ctrl.workerPodList[data.workerNode]; !ok {
		ctrl.workerPodList[data.workerNode] = []workerPodData{{podName: data.podName, workerNode: data.workerNode, podIp: data.podIp}}
	} else {
		ctrl.workerPodList[data.workerNode] = append(podData, workerPodData{podName: data.podName, workerNode: data.workerNode, podIp: data.podIp})
	}
}

func checkRatioSeparator(ratio string) bool {
	if !strings.Contains(ratio, RatioSeparator) {
		return false
	}
	return true
}

func validateCliServPodNum(ratio string) (error, bool) {
	var podNumber []string
	podNumber = strings.Split(ratio, RatioSeparator)
	clientPodNum, err := strconv.Atoi(podNumber[0])
	if err != nil {
		return err, false
	}

	serverPodNum, err := strconv.Atoi(podNumber[1])
	if err != nil {
		return err, false
	}

	if clientPodNum <= 0 || serverPodNum <= 0 {
		return fmt.Errorf("invalid pod numbers"), false
	}
	return nil, true
}

func deriveClientServerPodNum(ratio string) (int, int, string) {
	var podNumber []string
	var clientPodNum, serverPodNum int

	podNumber = strings.Split(ratio, RatioSeparator)
	clientPodNum, _ = strconv.Atoi(podNumber[0])
	serverPodNum, _ = strconv.Atoi(podNumber[1])
	if clientPodNum == serverPodNum && clientPodNum == 1 {
		return clientPodNum, serverPodNum, OneToOne
	}
	if (clientPodNum > serverPodNum) && serverPodNum == 1 {
		return clientPodNum, serverPodNum, ManyToOne
	}
	if clientPodNum == serverPodNum {
		return clientPodNum, serverPodNum, ManyToMany
	}
	return clientPodNum, serverPodNum, Invalid
}

func (ctrl *controller) executeTest() {
	switch ctrl.podRatioType {
	case OneToOne, ManyToMany:
		ctrl.execNToMTest()
	default:
		klog.Error("Invalid Pod Ratio")
	}
}

//execNToMTest executes testcase for N client, M server pods.
func (ctrl *controller) execNToMTest() {
	var uniqPodPair uniquePodPair

	go ctrl.formUniquePodPair()

	for {
		uniqPodPair = <-ctrl.podPairCh
		klog.V(3).Info("Unique PodPair :", uniqPodPair)
		if uniqPodPair.IsLastPodPair {
			break
		}
		ctrl.sendReqToSrv(uniqPodPair)
		//futureTimeStampForTestExec is sent to all pods, to start the test run at same time.
		if ctrl.futureTimeStampForTestExec == 0 {
			ctrl.futureTimeStampForTestExec = getTimeStampForPod()
		}
		ctrl.sendReqToClient(uniqPodPair)
	}

}

func (ctrl *controller) formUniquePodPair() {
	var uniqPodPair uniquePodPair
	lastPodPair := uniquePodPair{IsLastPodPair: true}

	var i = 0

	for {
		for key, value := range ctrl.workerPodList {
			unUsedPod, err := getUnusedPod(&value)
			ctrl.workerPodList[key] = value
			if err != nil {
				delete(ctrl.workerPodList, key)
				continue
			}
			i++

			if i == 1 {
				uniqPodPair.SrcPodIp = unUsedPod.podIp
				uniqPodPair.SrcPodName = unUsedPod.podName
			} else if i == 2 {
				uniqPodPair.DestPodIp = unUsedPod.podIp
				uniqPodPair.DestPodName = unUsedPod.podName
				i = 0
				ctrl.uniqPodPairList = append(ctrl.uniqPodPairList, uniqPodPair)
				ctrl.podPairCh <- uniqPodPair
			}
		}
		if len(ctrl.workerPodList) == 0 {
			ctrl.podPairCh <- lastPodPair
			break
		}
	}
}

func getUnusedPod(unusedPodList *[]workerPodData) (workerPodData, error) {
	var unusedPod workerPodData
	if len(*unusedPodList) == 0 {
		return unusedPod, errors.New("unused pod list empty")
	}
	numOfPods := len(*unusedPodList)
	//extract last pod of slice
	unusedPod = (*unusedPodList)[numOfPods-1]
	*unusedPodList = (*unusedPodList)[:numOfPods-1]
	return unusedPod, nil
}

func (ctrl *controller) sendReqToClient(uniqPodPair uniquePodPair) {
	klog.V(3).Infof("sending req to cli-pod: %s, protocol: %s, futureTimeStamp: %v, duration: %v",
		uniqPodPair.SrcPodName, ctrl.protocol, ctrl.futureTimeStampForTestExec, ctrl.testDuration)
	ctrl.startWork(uniqPodPair.SrcPodName, getHttpPath(ctrl.protocol, Client), ctrl.futureTimeStampForTestExec, "", uniqPodPair.DestPodIp)
}

func (ctrl *controller) sendReqToSrv(uniqPodPair uniquePodPair) {
	klog.V(3).Infof("sending req to srv-pod: %s, protocol: %s", uniqPodPair.DestPodName, ctrl.protocol)
	ctrl.startWork(uniqPodPair.DestPodName, getHttpPath(ctrl.protocol, Server), time.Now().Unix(), "1", "")
}

func getHttpPath(protocol string, mapIndex int) string {
	return httpPathMap[protocol][mapIndex]
}

func getTimeStampForPod() int64 {
	currTime := time.Now()
	initDelay := time.Second * time.Duration(initialDelayForTCExec)
	futureTime := currTime.Add(initDelay).Unix()
	return futureTime
}

func (ctrl *controller) collectMetrics(uniqPodPair uniquePodPair) *MetricResponse {
	var podName string

	switch ctrl.protocol {
	case ProtocolTCP, ProtocolUDP:
		podName = uniqPodPair.DestPodName
	case ProtocolHTTP:
		podName = uniqPodPair.SrcPodName
	}
	metricResp := ctrl.fetchMetrics(podName)
	return metricResp
}

// populateMetricVal stores the metric response received from worker pod in intermediate map.
func (ctrl *controller) populateMetricVal(uniqPodPair uniquePodPair, metricResp *MetricResponse) {
	switch ctrl.protocol {
	case ProtocolTCP, ProtocolUDP:
		ctrl.metricVal[uniqPodPair.DestPodName] = *metricResp
	case ProtocolHTTP:
		ctrl.metricVal[uniqPodPair.SrcPodName] = *metricResp
	}
	klog.V(3).Info("Metric in populateMetricVal:", *metricResp)
}

func (ctrl *controller) formNetPerfRespForDisp() NetworkPerfResp {
	var metricData NetworkPerfResp
	switch ctrl.protocol {
	case ProtocolTCP:
		ctrl.getMetricData(&metricData, TCPBW, Throughput)
		metricData.Protocol = ProtocolTCP
	case ProtocolUDP:
		ctrl.getMetricData(&metricData, UDPPps, PPS)
		ctrl.getMetricData(&metricData, UDPJitter, Jitter)
		ctrl.getMetricData(&metricData, UDPLatAvg, Latency)
		metricData.Protocol = ProtocolUDP
	case ProtocolHTTP:
		ctrl.getMetricData(&metricData, HTTPRespTime, ResponseTime)
		metricData.Protocol = ProtocolHTTP
	}
	metricData.Service = "P2P"
	metricData.Client_Server_Ratio = ctrl.podRatioType
	return metricData
}

func (ctrl *controller) getMetricData(data *NetworkPerfResp, metricIndex int, metricName string) {
	var dataElem measurementutil.DataItem
	dataElem.Data = make(map[string]float64)
	dataElem.Labels = make(map[string]string)
	dataElem.Labels["Metric"] = metricName
	ctrl.calculateMetricDataValue(&dataElem, metricIndex)
	dataElem.Unit = getUnit(dataElem.Labels["Metric"])
	data.DataItems = append(data.DataItems, dataElem)
	klog.V(3).Infof("Perfdata:%v", data)
}

func (ctrl *controller) calculateMetricDataValue(dataElem *measurementutil.DataItem, metricIndex int) {
	var aggrPodPairMetricSlice []float64
	var metricResp MetricResponse

	switch ctrl.podRatioType {
	case OneToOne:
		for _, metricResp = range ctrl.metricVal {
			if len(metricResp.Result) > 0 {
				dataElem.Data[value] = metricResp.Result[metricIndex]
			} else {
				dataElem.Data[value] = 0
			}
		}
		klog.Info("Metric value: ", dataElem.Data[value])
	case ManyToMany:
		for _, metricResp = range ctrl.metricVal {
			if len(metricResp.Result) > 0 {
				aggrPodPairMetricSlice = append(aggrPodPairMetricSlice, metricResp.Result[metricIndex])
			}
		}
		dataElem.Data[Perc05] = getPercentile(aggrPodPairMetricSlice, Percentile05)
		dataElem.Data[Perc50] = getPercentile(aggrPodPairMetricSlice, Percentile50)
		dataElem.Data[Perc95] = getPercentile(aggrPodPairMetricSlice, Percentile95)
		klog.Info("Aggregate Metric value: ", aggrPodPairMetricSlice)
	}
}

func (ctrl *controller) startWork(podName string, wrkType string, timestamp int64, numCls string, srvrIP string) {
	var resp WorkerResponse
	var params = make(map[string]string)
	params["duration"] = strconv.Itoa(ctrl.testDuration)
	params["timestamp"] = strconv.FormatInt(timestamp, 10)
	params["numCls"] = numCls
	params["destIP"] = srvrIP
	body := ctrl.messageWorker(podName, params, wrkType)
	if err := json.Unmarshal(*body, &resp); err != nil {
		klog.Error("Error unmarshalling metric response:", err)
		//TODO: Handle unmarshal error due to network issue?
	}
	klog.V(3).Info("Unmarshalled response to startWork:", resp)
}

func (ctrl *controller) fetchMetrics(podName string) *MetricResponse {
	var resp MetricResponse
	var params = make(map[string]string)
	body := ctrl.messageWorker(podName, params, "metrics")
	if err := json.Unmarshal(*body, &resp); err != nil {
		klog.Error("Error unmarshalling metric response:", err)
	}
	klog.V(3).Info("Unmarshalled metrics:", resp)
	return &resp
}

func (ctrl *controller) formMetricsForDisplay() {
	ctrl.retrieveMetricFromPods()
	if len(ctrl.metricRespPendingList) > 0 {
		wait.Poll(time.Duration(1)*time.Second, time.Duration(5)*time.Second, func() (bool, error) {
			return ctrl.retrieveMetricsFromPendingPods()
		})
	}

	t := len(ctrl.uniqPodPairList)
	mrpl := len(ctrl.metricRespPendingList)
	actualPodRatio := t - mrpl
	klog.Infof("Total uniqPodPairs: %v, response received from: %v", t, actualPodRatio)

	ctrl.networkPerfRespForDisp = ctrl.formNetPerfRespForDisp()
}

func (ctrl *controller) retrieveMetricsFromPendingPods() (bool, error) {
	if len(ctrl.metricRespPendingList) == 0 {
		return true, nil
	}

	ctrl.retrieveMetricFromPods()
	return false, nil
}

func (ctrl *controller) retrieveMetricFromPods() {
	var metricResp *MetricResponse
	var podList []uniquePodPair
	var pendingList []uniquePodPair

	if len(ctrl.metricRespPendingList) > 0 {
		podList = ctrl.metricRespPendingList
	} else {
		podList = ctrl.uniqPodPairList
	}
	for _, podPair := range podList {
		metricResp = ctrl.collectMetrics(podPair)
		if metricResp != nil && metricResp.Error == "" {
			ctrl.populateMetricVal(podPair, metricResp)
		} else {
			pendingList = append(pendingList, podPair)
		}
	}

	ctrl.metricRespPendingList = pendingList
}

func (ctrl *controller) messageWorker(podName string, params map[string]string, msgType string) *[]byte {
	req := ctrl.k8sClient.CoreV1().RESTClient().Get().
		Namespace(netperfNamespace).
		Resource("pods").
		Name(podName + ":" + WorkerListenPort).
		SubResource("proxy").Suffix(msgType)
	for k, v := range params {
		req = req.Param(k, v)
	}
	body, err := req.DoRaw(context.TODO())
	if err != nil {
		klog.Error("error calling ", msgType, ":", err.Error())
	} else {
		klog.V(3).Info("Response:")
		klog.V(3).Info(string(body))
	}
	return &body
}

func getUnit(metric string) string {
	return metricUnitMap[metric]
}

func (ctrl *controller) closeCh() {
	close(ctrl.podPairCh)
}

type float64Slice []float64

func (p float64Slice) Len() int           { return len(p) }
func (p float64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p float64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func getPercentile(values float64Slice, perc float64) float64 {
	ps := []float64{perc}

	scores := make([]float64, len(ps))
	size := len(values)
	if size > 0 {
		sort.Sort(values)
		for i, p := range ps {
			pos := p * float64(size+1) //ALTERNATIVELY, DROP THE +1
			if pos < 1.0 {
				scores[i] = values[0]
			} else if pos >= float64(size) {
				scores[i] = values[size-1]
			} else {
				lower := values[int(pos)-1]
				upper := values[int(pos)]
				scores[i] = lower + (pos-math.Floor(pos))*(upper-lower)
			}
		}
	}
	return scores[0]
}
