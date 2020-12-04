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
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog"
)

// Worker hold data required for facilitating measurement
type Worker struct {
	resultStatus chan string
	result       []string
	testDuration string
	protocol     string
	startedAt    time.Time
	futureTime   time.Time
}

// startWorkResponse is the response sent back to controller on starting a measurement
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
	// Command arguments for each protocol.This supports templates("{}"),
	// the value in template will be replaced by value in http request
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
	udpClientArguments = []string{"-c", "{destIP}", "-u", "-f", "K", "-l", "20", "-b", "1M", "-e", "-i", "1", "-t", "{duration}"}
	udpServerArguments = []string{"-s", "-f", "K", "-u", "-e", "-i", "{duration}", "-P", "{numCls}"}
	tcpServerArguments = []string{"-s", "-f", "K", "-i", "{duration}", "-P", "{numCls}"}
	tcpClientArguments = []string{"-c", "{destIP}", "-f", "K", "-l", "20", "-b", "1M", "-i", "1", "-t", "{duration}"}
	// Siege command args:
	// -d1 : random delay between 0 to 1 sec
	// -t<duration>S : run test for <duration> seconds
	// -c1 : one concurrent user
	httpClientCmd = []string{"http://" + "{destIP}" + ":" + httpPort + "/test", "-d1", "-t" + "{duration}" + "S", "-c1"}
)

func NewWorker() *Worker {
	return &Worker{}
}

// Start starts the worker
func (w *Worker) Start() {
	w.resultStatus = make(chan string)
	w.listenToController()
}

func (w *Worker) listenToController() {
	h := map[string]func(http.ResponseWriter, *http.Request){
		"/startTCPServer":  w.StartTCPServer,
		"/startTCPClient":  w.StartTCPClient,
		"/startUDPServer":  w.StartUDPServer,
		"/startUDPClient":  w.StartUDPClient,
		"/startHTTPServer": w.StartHTTPServer,
		"/startHTTPClient": w.StartHTTPClient,
		"/metrics":         w.Metrics,
	}
	w.startListening(workerListenPort, h)
}

func (w *Worker) startListening(port string, handlers map[string]func(http.ResponseWriter, *http.Request)) {
	for urlPath, handler := range handlers {
		http.HandleFunc(urlPath, handler)
	}
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		klog.Fatalf("Failed starting http server for port: %v, Error: %v", port, err)
	}
}

// Metrics returns the metrics collected
func (w *Worker) Metrics(rw http.ResponseWriter, req *http.Request) {
	var reply metricResponse
	var status string
	select {
	case status = <-w.resultStatus:
		if status != "OK" {
			klog.Errorf("Error collecting metrics: %v", status)
			reply.Error = "metrics collection failed: " + status
			w.sendResponse(reply, rw, http.StatusInternalServerError)
			return
		}
		klog.Info("Metrics collected")
	default:
		klog.Info("Metric collection in progress")
		reply.Error = "metric collection in progress"
		w.sendResponse(reply, rw, http.StatusProcessing)
		return
	}
	parsedResult, err := ParseResult(w.protocol, w.result, w.testDuration)
	if err != nil {
		reply.Error = err.Error()
		w.sendResponse(reply, rw, http.StatusInternalServerError)
		return
	}
	reply.Result = parsedResult
	timeDifference := w.futureTime.Sub(w.startedAt)
	reply.WorkerStartTime = "StartedAt: " + w.startedAt.String() + " FutureTime: " + w.futureTime.String() + " diffTime: " + timeDifference.String()
	w.sendResponse(reply, rw, http.StatusOK)

}

func (w *Worker) sendResponse(resp interface{}, rw http.ResponseWriter, statusCode int) {
	rw.Header().Set("Content-Type", "application/json")
	klog.V(3).Infof("Marshalled Resp: %v", resp)
	b, err := json.Marshal(resp)
	if err != nil {
		klog.Infof("Error marshalling to json: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(statusCode)
	rw.Write(b)
}

// StartHTTPServer starts an http server for http measurements
func (w *Worker) StartHTTPServer(rw http.ResponseWriter, req *http.Request) {
	klog.Info("Starting HTTP Server")
	go w.startListening(httpPort, map[string]func(http.ResponseWriter, *http.Request){"/test": w.Handler})
	w.sendResponse(startWorkResponse{}, rw, http.StatusOK)
}

// StartTCPServer starts iperf server for tcp measurements
func (w *Worker) StartTCPServer(rw http.ResponseWriter, req *http.Request) {
	klog.Info("Starting TCP Server")
	w.startWork(rw, req, ProtocolTCP, "iperf", tcpServerArguments)
}

// StartUDPServer starts iperf server for udp measurements
func (w *Worker) StartUDPServer(rw http.ResponseWriter, req *http.Request) {
	klog.Info("Starting UDP Server")
	w.startWork(rw, req, ProtocolUDP, "iperf", udpServerArguments)
}

// StartHTTPClient starts an http client for http measurements
func (w *Worker) StartHTTPClient(rw http.ResponseWriter, req *http.Request) {
	klog.Info("Starting HTTP Client")
	w.startWork(rw, req, ProtocolHTTP, "siege", httpClientCmd)
}

// StartTCPClient starts iperf client for tcp measurements
func (w *Worker) StartTCPClient(rw http.ResponseWriter, req *http.Request) {
	klog.Info("Starting TCP Client")
	w.startWork(rw, req, ProtocolTCP, "iperf", tcpClientArguments)
}

// StartUDPClient starts iperf client for udp measurements
func (w *Worker) StartUDPClient(rw http.ResponseWriter, req *http.Request) {
	klog.Info("Starting UDP Client")
	w.startWork(rw, req, ProtocolUDP, "iperf", udpClientArguments)
}

// Handler handles http requests for http measurements
func (w *Worker) Handler(rw http.ResponseWriter, req *http.Request) {
	w.sendResponse("hi", rw, http.StatusOK)
}

func (w *Worker) startWork(rw http.ResponseWriter, req *http.Request, protocol string, cmd string, args []string) {
	// Below loop populates template parameters with actual values from the http request object
	for i, arg := range args {
		if idx := strings.Index(arg, "{"); idx != -1 {
			param := arg[strings.Index(arg, "{")+1 : strings.Index(arg, "}")]
			klog.Infof("arg with template: %v", param)
			valMap, err := GetValuesFromURL(req, []string{param})
			if err != nil {
				w.sendResponse(startWorkResponse{error: err.Error()}, rw, http.StatusBadRequest)
				return
			}
			args[i] = strings.Replace(arg, "{"+param+"}", valMap[param], 1)
			klog.Infof("Value after resolving template: %v", args[i])
		}
	}
	valMap, err := GetValuesFromURL(req, []string{"timestamp", "duration"})
	if err != nil {
		w.sendResponse(startWorkResponse{error: err.Error()}, rw, http.StatusBadRequest)
		return
	}
	tsint, err := strconv.ParseInt(valMap["timestamp"], 10, 64)
	if err != nil {
		klog.Infof("Invalid timestamp: %v %v", valMap["timestamp"], err)
	}
	go w.schedule(protocol, tsint, valMap["duration"], cmd, args)
	w.sendResponse(startWorkResponse{}, rw, http.StatusOK)
	return
}

func (w *Worker) schedule(protocol string, futureTimestamp int64, duration string, command string, args []string) {
	//If future time is in past,run immediately
	klog.Infof("About to wait for futuretime: %v", futureTimestamp)
	klog.Infof("Current time: %v", time.Now().Unix())
	time.Sleep(time.Duration(futureTimestamp-time.Now().Unix()) * time.Second)
	w.startedAt = time.Now()
	w.futureTime = time.Unix(futureTimestamp, 0)
	w.testDuration = duration
	w.protocol = protocol
	w.execCmd(duration, command, args)
}

func (w *Worker) execCmd(duration string, command string, args []string) {
	cmd := exec.Command(command, args...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		klog.Errorf("unable to obtain Stdout: %v", err)
		w.resultStatus <- err.Error()
		return
	}
	eout, err := cmd.StderrPipe()
	if err != nil {
		klog.Errorf("unable to obtain Stderr: %v", err)
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
		if l == "" {
			continue
		}
		w.result = append(w.result, l)
	}
	klog.Info("Command executed,sending result back")
	if err := scanner.Err(); err != nil {
		klog.Errorf("Error: %v", err)
		w.resultStatus <- err.Error()
		return
	}
	w.resultStatus <- "OK"
}
