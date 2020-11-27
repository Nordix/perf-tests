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

// Package worker implements the worker related activities like starting TCP/UDP/HTTP client/server
// and collecting the metric output to be returned to the controller when requested.
package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"k8s.io/perf-tests/util-images/network/netperfbenchmark/pkg/util"

	"k8s.io/klog"
)

// Worker hold data required for facilitating measurement
type Worker struct {
	resultStatus chan string
	result       []string
	tstDuration  string
	tstProtocol  string
	startedAt    int64
	futureTime   int64
}

// startResponse is the response sent back to controller on starting a measurement
type startWorkResponse struct {
	error string
}

// metricResponse is the response sent back to controller after collecting measurement
type metricResponse struct {
	Result          []float64
	WorkerStartTime string
	Error           string
}

// http  listen ports
const (
	workerListenPort = "5003"
	httpPort         = "5301"
)

var (
	//Command arguments for each protocol.This supports templates("{}"),the value in template will be replaced by value in http request
	// Iperf command args:
	// -c <destIP> : connect to destIP(as client)
	// -f K : report format KBytes/sec
	// -l 20 : read/write buffer size 20 bytes
	// -b 1M : bandwidth 1 Mbits/sec
	// -i 1 : report stats every 1 sec
	// -t duration : run  <duration> seconds
	// -u : for udp measurement
	// -e : enhanced reports, gives more metrics for udp
	// -s : run in server mode
	// -P numCls: handle <numCls> number of clients before disconnecting
	udpClientCmd = []string{"-c", "{destIP}", "-u", "-f", "K", "-l", "20", "-b", "1M", "-e", "-i", "1", "-t", "{duration}"}
	udpServerCmd = []string{"-s", "-f", "K", "-u", "-e", "-i", "{duration}", "-P", "{numCls}"}
	tcpServerCmd = []string{"-s", "-f", "K", "-i", "{duration}", "-P", "{numCls}"}
	tcpClientCmd = []string{"-c", "{destIP}", "-f", "K", "-l", "20", "-b", "1M", "-i", "1", "-t", "{duration}"}
	// Siege command args:
	// -d1 : random delay between 0 to 1 sec
	// -t<duration>S : run test for <duration> seconds
	// -c1 : one concurrent user
	httpClientCmd = []string{"http://" + "{destIP}" + ":" + httpPort + "/test", "-d1", "-t" + "{duration}" + "S", "-c1"}
)

// Start worker
func (w *Worker) Start() {
	w.resultStatus = make(chan string)
	w.listenToController()
}

func (w *Worker) listenToController() {
	h := map[string]func(http.ResponseWriter, *http.Request){"/startTCPServer": w.StartTCPServer,
		"/startTCPClient": w.StartTCPClient, "/startUDPServer": w.StartUDPServer, "/startUDPClient": w.StartUDPClient,
		"/startHTTPServer": w.StartHTTPServer, "/startHTTPClient": w.StartHTTPClient, "/metrics": w.Metrics}
	w.startListening(workerListenPort, h)
	klog.Info("Started listening to Server")
}

func (w *Worker) startListening(port string, handlers map[string]func(http.ResponseWriter, *http.Request)) {
	klog.Info("About to listen to controller requests")
	for p, h := range handlers {
		http.HandleFunc(p, h)
	}
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		klog.Fatalf("Failed starting http server for port: %v, Error: %v", port, err)
	}
}

// Metrics returns the metrics collected
func (w *Worker) Metrics(res http.ResponseWriter, req *http.Request) {
	var reply metricResponse
	klog.Info("In metrics")
	var stat string
	select {
	case stat = <-w.resultStatus:
		if stat != "OK" {
			klog.Error("Error collecting metrics:", stat)
			reply.Error = "metrics collection failed:" + stat
			w.createResponse(reply, &res)
			return
		}
		klog.Info("Metrics collected")
	default:
		klog.Info("Metric collection in progress")
		reply.Error = "metric collection in progress"
		w.createResponse(reply, &res)
		return
	}
	o, err := util.ParseResult(w.tstProtocol, w.result, w.tstDuration)
	if err != nil {
		reply.Error = err.Error()
		w.createResponse(reply, &res)
		return
	}
	reply.Result = o
	//startime and other variables below can be removed in future, for debugging during initial runs
	stStr := strconv.FormatInt(w.startedAt, 10)
	ftStr := strconv.FormatInt(w.futureTime, 10)
	dtStr := strconv.FormatInt(w.futureTime-w.startedAt, 10)
	reply.WorkerStartTime = "StartedAt:" + stStr + " FutureTime:" + ftStr + " diffTime:" + dtStr
	w.createResponse(reply, &res)
}

func (w *Worker) createResponse(resp interface{}, wr *http.ResponseWriter) {
	klog.Info("Inside create resp")
	(*wr).Header().Set("Content-Type", "application/json")
	(*wr).WriteHeader(http.StatusOK)
	klog.Info("Marshalled Resp:", resp)
	b, err := json.Marshal(resp)
	if err != nil {
		klog.Info("Error marshalling to json:", err)
	}
	(*wr).Write(b)
}

// StartTCPClient starts iperf client for tcp measurements
func (w *Worker) StartTCPClient(res http.ResponseWriter, req *http.Request) {
	klog.Info("In StartTCPClient")
	w.startWork(&res, req, util.ProtocolTCP, "iperf", tcpClientCmd)
}

// StartTCPServer starts iperf server for tcp measurements
func (w *Worker) StartTCPServer(res http.ResponseWriter, req *http.Request) {
	klog.Info("In StartTCPServer")
	w.startWork(&res, req, util.ProtocolTCP, "iperf", tcpServerCmd)
}

// StartUDPServer starts iperf server for udp measurements
func (w *Worker) StartUDPServer(res http.ResponseWriter, req *http.Request) {
	klog.Info("In StartUDPServer")
	w.startWork(&res, req, util.ProtocolUDP, "iperf", udpServerCmd)
}

// StartUDPClient starts iperf server for udp measurements
func (w *Worker) StartUDPClient(res http.ResponseWriter, req *http.Request) {
	klog.Info("In StartUDPClient")
	w.startWork(&res, req, util.ProtocolUDP, "iperf", udpClientCmd)
}

// StartHTTPServer starts an http server for http measurements
func (w *Worker) StartHTTPServer(res http.ResponseWriter, req *http.Request) {
	klog.Info("In StartHTTPServer")
	go w.startListening(httpPort, map[string]func(http.ResponseWriter, *http.Request){"/test": w.Handler})
	w.createResponse(startWorkResponse{}, &res)
}

// StartHTTPClient starts an http client for http measurements
func (w *Worker) StartHTTPClient(res http.ResponseWriter, req *http.Request) {
	klog.Info("In StartHTTPClient")
	w.startWork(&res, req, util.ProtocolHTTP, "siege", httpClientCmd)
}

// Handler handles http requests for http measurements
func (w *Worker) Handler(res http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(res, "hi\n")
}

func (w *Worker) startWork(res *http.ResponseWriter, req *http.Request, protocol string, cmd string, args []string) {
	// Below loop populates template parameters with actual values from the http request object
	for i, arg := range args {
		if idx := strings.Index(arg, "{"); idx != -1 {
			param := arg[strings.Index(arg, "{")+1 : strings.Index(arg, "}")]
			klog.Info("arg with template:", param)
			valMap, err := util.GetValuesFromURL(req, []string{param})
			if err != nil {
				w.createResponse(startWorkResponse{error: err.Error()}, res)
				return
			}
			args[i] = strings.Replace(arg, "{"+param+"}", valMap[param], 1)
			klog.Info("Value after resolving template:", args[i])
		}
	}
	valMap, err := util.GetValuesFromURL(req, []string{"timestamp", "duration"})
	if err != nil {
		w.createResponse(startWorkResponse{error: err.Error()}, res)
		return
	}
	tsint, err := strconv.ParseInt(valMap["timestamp"], 10, 64)
	if err != nil {
		klog.Info("Invalid timestamp:", valMap["timestamp"], " ", err)
	}
	go w.schedule(protocol, tsint, valMap["duration"], cmd, args)
	w.createResponse(startWorkResponse{}, res)
	return
}

func (w *Worker) schedule(protocol string, futureTimestamp int64, duration string, command string, args []string) {
	//If future time is in past,run immediately
	klog.Info("About to wait for futuretime:", futureTimestamp)
	klog.Info("Current time:", time.Now().Unix())
	time.Sleep(time.Duration(futureTimestamp-time.Now().Unix()) * time.Second)
	w.startedAt = time.Now().Unix()
	w.futureTime = futureTimestamp
	w.tstDuration = duration
	w.tstProtocol = protocol
	w.execCmd(duration, command, args)
}

func (w *Worker) execCmd(duration string, command string, args []string) {
	cmd := exec.Command(command, args...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		klog.Error("unable to obtain Stdout:", err)
		w.resultStatus <- err.Error()
		return
	}
	eout, err := cmd.StderrPipe()
	if err != nil {
		klog.Error("unable to obtain Stderr:", err)
		w.resultStatus <- err.Error()
		return
	}
	multiRdr := io.MultiReader(out, eout)
	err = cmd.Start()
	if err != nil {
		w.resultStatus <- err.Error()
	}
	w.scanOutput(&multiRdr)
}

func (w *Worker) scanOutput(out *io.Reader) {
	scanner := bufio.NewScanner(*out)
	klog.Info("Starting scan for output")
	for scanner.Scan() {
		l := scanner.Text()
		// klog.Info(line)
		if l == "" {
			continue
		}
		w.result = append(w.result, l)
	}
	klog.Info("Command executed,sending result back")
	if err := scanner.Err(); err != nil {
		klog.Error("Error", err)
		w.resultStatus <- err.Error()
		return
	}
	w.resultStatus <- "OK"
}
