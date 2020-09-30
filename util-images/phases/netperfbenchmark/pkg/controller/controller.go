package controller

import (
	"k8s.io/perf-tests/util-images/phases/netperfbenchmark/api"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"sync"

	"k8s.io/klog"
)

var globalLock sync.Mutex

//ControllerRPC service that exposes RegisterWorkerPod API for clients
type ControllerRPC int

var workerPodList map[string][]api.WorkerPodData

const ratioSeparator = ":"

//Client-To-Server Pod ratio indicator
const (
	OneToOne = 1
	ManyToOne = 2
	ManyToMany = 3
	InvalidRatio = 4
)

func Start() {
	workerPodList = make(map[string][]api.WorkerPodData)
	InitializeServerRPC(api.ControllerRpcSvcPort)
}

func InitializeServerRPC(port string) {
	baseObject := new(ControllerRPC)
	err := rpc.Register(baseObject)
	if err != nil {
		klog.Fatalf("failed to register rpc", err)
	}
	rpc.HandleHTTP()
	listener, e := net.Listen("tcp", ":"+port)
	if e != nil {
		klog.Fatalf("listen error:", e)
	}
	err = http.Serve(listener, nil)
	if err != nil {
		klog.Fatalf("failed start server", err)
	}
}

func (t *ControllerRPC) RegisterWorkerPod(data *api.WorkerPodData, reply *api.WorkerPodRegReply) error {
	globalLock.Lock()
	defer globalLock.Unlock()

	if podData, ok := workerPodList[data.WorkerNode] ; !ok {
		workerPodList[data.WorkerNode] = []api.WorkerPodData{{PodName: data.PodName, WorkerNode: data.WorkerNode, PodIp: data.PodIp}}
		reply.Response = "Hi"
		return nil
	} else {
		workerPodList[data.WorkerNode] = append(podData, api.WorkerPodData{PodName: data.PodName, WorkerNode: data.WorkerNode, PodIp: data.PodIp})
		return nil
	}
}

func ExecuteTest(ratio string, duration string, protocol string) {
	var clientPodNum , serverPodNum, ratioType int

	//Get the client-server pod numbers
	clientPodNum , serverPodNum, ratioType = deriveClientServerPodNum(ratio)
	timeduration , _ := strconv.Atoi(duration)

	switch ratioType {
	case OneToOne:
		executeOneToOneTest(timeduration, protocol)
	case ManyToOne:
		executeManyToOneTest(clientPodNum , serverPodNum, timeduration, protocol)
	case ManyToMany:
		executeManyToManyTest(clientPodNum , serverPodNum, timeduration, protocol)
	default:
		klog.Fatalf("Invalid Pod Ratio")
	}
}

func deriveClientServerPodNum(ratio string) (int, int, int) {
	var podNumber []string
	var clientPodNum , serverPodNum, ratioType int
	if strings.Contains(ratio, ratioSeparator) {
		podNumber = strings.Split(ratio, ratioSeparator)
		clientPodNum, _ = strconv.Atoi(podNumber[0])
		serverPodNum, _ = strconv.Atoi(podNumber[1])

		if clientPodNum == serverPodNum {
			ratioType = OneToOne
		}
		if (clientPodNum > serverPodNum) && serverPodNum == 1 {
			ratioType = ManyToOne
		}
		if clientPodNum > 1 && serverPodNum > 1 {
			ratioType = ManyToMany
		}

		return clientPodNum, serverPodNum, ratioType
	}

	klog.Fatalf("Invalid Pod Ratio")
	return -1,-1,InvalidRatio
}

//Select one client , one server pod.
func executeOneToOneTest(duration int, protocol string) {

	if len(workerPodList) == 0 || len(workerPodList) == 1 {
		klog.Fatalf("Either no worker-node or one worker node exist. Can't choose pods from same worker node")
		return
	}

}

//Select N clients , one server pod.
func executeManyToOneTest(clientPodNum int, serverPodNum int, duration int, protocol string) {

}

//Select N clients , M server pod.
func executeManyToManyTest(clientPodNum int, serverPodNum int, duration int, protocol string) {

}