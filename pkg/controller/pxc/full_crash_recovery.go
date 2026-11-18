package pxc

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	v1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	ErrNotAllPXCPodsRunning = errors.New("not all pxc pods are running")
	logLinesRequired        = int64(8)
)

const (
	logPrefix = `#####################################################LAST_LINE`
	// zeroUUID is what grastate.dat holds before a node has any cluster history.
	zeroUUID = "00000000-0000-0000-0000-000000000000"
)

// recoveryPosition is what a pod waiting in full-crash recovery reports in its LAST_LINE.
type recoveryPosition struct {
	seq int64
	// uuid is the wsrep cluster state UUID of the pod's data, or "" when the pod runs an
	// entrypoint that does not report it.
	uuid string
}

func (r *ReconcilePerconaXtraDBCluster) recoverFullClusterCrashIfNeeded(ctx context.Context, cr *v1.PerconaXtraDBCluster) error {
	if cr.Spec.PXC.Size <= 0 {
		return nil
	}

	err := r.checkIfPodsRunning(cr)
	if err != nil {
		if err == ErrNotAllPXCPodsRunning {
			return nil
		}
		return err
	}

	isWaiting, _, err := r.isPodWaitingForRecovery(cr.Namespace, cr.Name+"-pxc-0")
	if err != nil {
		return errors.Wrap(err, "failed to check if pxc pod 0 is waiting for recovery")
	}

	if isWaiting {
		return r.doFullCrashRecovery(ctx, cr.Name, cr.Namespace, int(cr.Spec.PXC.Size))
	}

	return nil
}

func (r *ReconcilePerconaXtraDBCluster) isPodWaitingForRecovery(namespace, podName string) (bool, recoveryPosition, error) {
	logOpts := &corev1.PodLogOptions{
		Container: "pxc",
		TailLines: &logLinesRequired,
	}
	logLines, err := r.clientcmd.PodLogs(namespace, podName, logOpts)
	if err != nil {
		return false, recoveryPosition{seq: -1}, errors.Wrapf(err, "get logs from %s pod", podName)
	}

	for i := len(logLines) - 1; i >= 0; i-- {
		if strings.HasPrefix(logLines[i], logPrefix) {
			pos, err := parseRecoveryPosition(logLines[i])
			return true, pos, err
		}
	}

	return false, recoveryPosition{seq: -1}, nil
}

// parseRecoveryPosition reads "<prefix>:<node>:<seqno>:<suffix>" (entrypoints before PCD-9737)
// or "<prefix>:<node>:<seqno>:<uuid>:<suffix>".
func parseRecoveryPosition(log string) (recoveryPosition, error) {
	logsSplitted := strings.Split(log, ":")
	if len(logsSplitted) != 4 && len(logsSplitted) != 5 {
		return recoveryPosition{seq: -1}, errors.New("invalid log format. Log: " + log)
	}

	seq, err := strconv.ParseInt(logsSplitted[2], 10, 64)
	if err != nil {
		return recoveryPosition{seq: -1}, errors.Wrapf(err, "parse sequence %s", logsSplitted[2])
	}

	pos := recoveryPosition{seq: seq}
	if len(logsSplitted) == 5 {
		pos.uuid = strings.TrimSpace(logsSplitted[3])
	}
	return pos, nil
}

// conflictingHistories returns the distinct cluster UUIDs reported by the pods when more than
// one is present, nil otherwise. Pods that report no UUID (older entrypoint) or the zero UUID
// (no history yet) are left out: they carry no history that could conflict.
//
// PCD-9737: seqno is only comparable between pods that share a cluster history. A pod whose
// data comes from another cluster (an old datadir left from an earlier install, or a split
// brain that bootstrapped a new cluster) can report a higher seqno than the real data, and
// bootstrapping from it replaces every other node's data through SST.
func conflictingHistories(positions map[string]recoveryPosition) []string {
	seen := map[string]bool{}
	uuids := []string{}
	for _, pos := range positions {
		if pos.uuid == "" || pos.uuid == zeroUUID || seen[pos.uuid] {
			continue
		}
		seen[pos.uuid] = true
		uuids = append(uuids, pos.uuid)
	}
	if len(uuids) < 2 {
		return nil
	}
	sort.Strings(uuids)
	return uuids
}

func (r *ReconcilePerconaXtraDBCluster) doFullCrashRecovery(ctx context.Context, crName, namespace string, pxcSize int) error {
	maxSeq := int64(-100)
	maxSeqPod := ""
	positions := make(map[string]recoveryPosition, pxcSize)

	for i := 0; i < pxcSize; i++ {
		podName := fmt.Sprintf("%s-pxc-%d", crName, i)
		isPodWaitingForRecovery, pos, err := r.isPodWaitingForRecovery(namespace, podName)
		if err != nil {
			return errors.Wrapf(err, "parse %s pod logs", podName)
		}

		if !isPodWaitingForRecovery {
			return nil
		}

		positions[podName] = pos
		if pos.seq > maxSeq {
			maxSeq = pos.seq
			maxSeqPod = podName
		}
	}
	log := logf.FromContext(ctx)
	if uuids := conflictingHistories(positions); uuids != nil {
		for podName, pos := range positions {
			log.Info("Full crash recovery position", "pod", podName, "seq", pos.seq, "uuid", pos.uuid)
		}
		return errors.Errorf("full cluster crash: pods report different cluster histories %v, "+
			"refusing to bootstrap automatically; pick the node to bootstrap from manually", uuids)
	}
	log.Info("We are in full cluster crash, starting recovery")
	log.Info("Results of scanning sequences", "pod", maxSeqPod, "maxSeq", maxSeq)

	pod := &corev1.Pod{}
	err := r.client.Get(context.TODO(), types.NamespacedName{
		Namespace: namespace,
		Name:      maxSeqPod,
	}, pod)
	if err != nil {
		return errors.Wrap(err, "get pods defenition")
	}

	stderrBuf := &bytes.Buffer{}
	err = r.clientcmd.Exec(pod, "pxc", []string{"/bin/sh", "-c", "kill -s USR1 1"}, nil, nil, stderrBuf, false)
	if err != nil {
		return errors.Wrap(err, "exec command in pod")
	}

	if stderrBuf.Len() != 0 {
		return errors.New("invalid exec command return: " + stderrBuf.String())
	}

	// sleep there a little to start script and do not send
	// a lot of signals to the same pod
	time.Sleep(30 * time.Second)

	return nil
}

func (r *ReconcilePerconaXtraDBCluster) checkIfPodsRunning(cr *v1.PerconaXtraDBCluster) error {
	for i := 0; i < int(cr.Spec.PXC.Size); i++ {
		podName := fmt.Sprintf("%s-pxc-%d", cr.Name, i)
		ok, err := r.clientcmd.IsPodRunning(cr.Namespace, podName)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return ErrNotAllPXCPodsRunning
			}
			return errors.Wrapf(err, "can't check pod %s state", podName)
		}
		if !ok {
			return ErrNotAllPXCPodsRunning
		}
	}
	return nil
}
