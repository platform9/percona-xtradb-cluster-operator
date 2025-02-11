package pxcrestore

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/percona/percona-xtradb-cluster-operator/clientcmd"
	api "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/k8s"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/pxc/app/binlogcollector"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/pxc/backup"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/pxc/backup/storage"
	"github.com/percona/percona-xtradb-cluster-operator/version"
)

// Add creates a new PerconaXtraDBClusterRestore Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	r, err := newReconciler(mgr)
	if err != nil {
		return err
	}
	return add(mgr, r)
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) (reconcile.Reconciler, error) {
	sv, err := version.Server()
	if err != nil {
		return nil, fmt.Errorf("get version: %v", err)
	}

	cli, err := clientcmd.NewClient()
	if err != nil {
		return nil, errors.Wrap(err, "create clientcmd")
	}

	return &ReconcilePerconaXtraDBClusterRestore{
		client:               mgr.GetClient(),
		clientcmd:            cli,
		scheme:               mgr.GetScheme(),
		serverVersion:        sv,
		newStorageClientFunc: storage.NewClient,
	}, nil
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	return builder.ControllerManagedBy(mgr).
		Named("pxcrestore-controller").
		Watches(&api.PerconaXtraDBClusterRestore{}, &handler.EnqueueRequestForObject{}).
		Complete(r)
}

var _ reconcile.Reconciler = &ReconcilePerconaXtraDBClusterRestore{}

// ReconcilePerconaXtraDBClusterRestore reconciles a PerconaXtraDBClusterRestore object
type ReconcilePerconaXtraDBClusterRestore struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client    client.Client
	clientcmd *clientcmd.Client
	scheme    *runtime.Scheme

	serverVersion *version.ServerVersion

	newStorageClientFunc storage.NewClientFunc
}

// Reconcile reads that state of the cluster for a PerconaXtraDBClusterRestore object and makes changes based on the state read
// and what is in the PerconaXtraDBClusterRestore.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcilePerconaXtraDBClusterRestore) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := logf.FromContext(ctx)

	rr := reconcile.Result{}

	cr := &api.PerconaXtraDBClusterRestore{}
	err := r.client.Get(context.TODO(), request.NamespacedName, cr)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			return rr, nil
		}
		// Error reading the object - requeue the request.
		return rr, err
	}
	if cr.Status.State != api.RestoreNew {
		return rr, nil
	}

	log.Info("backup restore request")

	err = r.setStatus(cr, api.RestoreStarting, "")
	if err != nil {
		return rr, errors.Wrap(err, "set status")
	}
	rJobsList := &api.PerconaXtraDBClusterRestoreList{}
	err = r.client.List(
		context.TODO(),
		rJobsList,
		&client.ListOptions{
			Namespace: cr.Namespace,
		},
	)
	if err != nil {
		return rr, errors.Wrap(err, "get restore jobs list")
	}

	returnMsg := fmt.Sprintf(backupRestoredMsg, cr.Name, cr.Spec.PXCCluster, cr.Name)

	defer func() {
		status := api.BcpRestoreStates(api.RestoreSucceeded)
		if err != nil {
			status = api.RestoreFailed
			returnMsg = err.Error()
		}
		err := r.setStatus(cr, status, returnMsg)
		if err != nil {
			return
		}
	}()

	for _, j := range rJobsList.Items {
		if j.Spec.PXCCluster == cr.Spec.PXCCluster &&
			j.Name != cr.Name && j.Status.State != api.RestoreFailed &&
			j.Status.State != api.RestoreSucceeded {
			err = errors.Errorf("unable to continue, concurent restore job %s running now.", j.Name)
			return rr, err
		}
	}

	err = cr.CheckNsetDefaults()
	if err != nil {
		return rr, err
	}

	cluster := new(api.PerconaXtraDBCluster)
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: cr.Spec.PXCCluster, Namespace: cr.Namespace}, cluster)
	if err != nil {
		err = errors.Wrapf(err, "get cluster %s", cr.Spec.PXCCluster)
		return rr, err
	}
	clusterOrig := cluster.DeepCopy()

	err = cluster.CheckNSetDefaults(r.serverVersion, log)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("wrong PXC options: %v", err)
	}

	bcp, err := r.getBackup(ctx, cr)
	if err != nil {
		return rr, errors.Wrap(err, "get backup")
	}

	if cr.Spec.PITR != nil {
		err = backup.CheckPITRErrors(ctx, r.client, r.clientcmd, cluster)
		if err != nil {
			return reconcile.Result{}, err
		}

		annotations := cr.GetAnnotations()
		_, unsafePITR := annotations[api.AnnotationUnsafePITR]
		cond := meta.FindStatusCondition(bcp.Status.Conditions, api.BackupConditionPITRReady)
		if cond != nil && cond.Status == metav1.ConditionFalse && !unsafePITR {
			msg := fmt.Sprintf("Backup doesn't guarantee consistent recovery with PITR. Annotate PerconaXtraDBClusterRestore with %s to force it.", api.AnnotationUnsafePITR)
			err = errors.New(msg)
			return reconcile.Result{}, nil
		}
	}

	err = r.validate(ctx, cr, bcp, cluster)
	if err != nil {
		err = errors.Wrap(err, "failed to validate restore job")
		return rr, err
	}

	log.Info("stopping cluster", "cluster", cr.Spec.PXCCluster)
	err = r.setStatus(cr, api.RestoreStopCluster, "")
	if err != nil {
		err = errors.Wrap(err, "set status")
		return rr, err
	}
	err = k8s.PauseClusterWithWait(ctx, r.client, cluster, true)
	if err != nil {
		err = errors.Wrapf(err, "stop cluster %s", cluster.Name)
		return rr, err
	}

	log.Info("starting restore", "cluster", cr.Spec.PXCCluster, "backup", cr.Spec.BackupName)
	err = r.setStatus(cr, api.RestoreRestore, "")
	if err != nil {
		err = errors.Wrap(err, "set status")
		return rr, err
	}

	err = r.restore(ctx, cr, bcp, cluster)
	if err != nil {
		err = errors.Wrap(err, "run restore")
		return rr, err
	}

	if cluster.Spec.Backup.PITR.Enabled {
		if err := binlogcollector.InvalidateCache(ctx, r.client, cluster); err != nil {
			log.Error(err, "failed to invalidate binlog collector cache")
		}
	}

	log.Info("starting cluster", "cluster", cr.Spec.PXCCluster)
	err = r.setStatus(cr, api.RestoreStartCluster, "")
	if err != nil {
		err = errors.Wrap(err, "set status")
		return rr, err
	}

	if cr.Spec.PITR != nil {
		oldSize := cluster.Spec.PXC.Size
		oldUnsafePXCSize := cluster.Spec.Unsafe.PXCSize
		oldUnsafeProxySize := cluster.Spec.Unsafe.ProxySize

		var oldProxySQLSize int32
		if cluster.Spec.ProxySQL != nil {
			oldProxySQLSize = cluster.Spec.ProxySQL.Size
		}
		var oldHAProxySize int32
		if cluster.Spec.HAProxy != nil {
			oldHAProxySize = cluster.Spec.HAProxy.Size
		}

		cluster.Spec.Unsafe.PXCSize = true
		cluster.Spec.Unsafe.ProxySize = true
		cluster.Spec.PXC.Size = 1

		if cluster.Spec.ProxySQL != nil {
			cluster.Spec.ProxySQL.Size = 0
		}
		if cluster.Spec.HAProxy != nil {
			cluster.Spec.HAProxy.Size = 0
		}

		if err := k8s.UnpauseClusterWithWait(ctx, r.client, cluster); err != nil {
			return rr, errors.Wrap(err, "restart cluster for pitr")
		}

		log.Info("point-in-time recovering", "cluster", cr.Spec.PXCCluster)
		err = r.setStatus(cr, api.RestorePITR, "")
		if err != nil {
			return rr, errors.Wrap(err, "set status")
		}

		err = r.pitr(ctx, cr, bcp, cluster)
		if err != nil {
			return rr, errors.Wrap(err, "run pitr")
		}

		cluster.Spec.PXC.Size = oldSize
		cluster.Spec.Unsafe.PXCSize = oldUnsafePXCSize
		cluster.Spec.Unsafe.ProxySize = oldUnsafeProxySize

		if cluster.Spec.ProxySQL != nil {
			cluster.Spec.ProxySQL.Size = oldProxySQLSize
		}
		if cluster.Spec.HAProxy != nil {
			cluster.Spec.HAProxy.Size = oldHAProxySize
		}

		log.Info("starting cluster", "cluster", cr.Spec.PXCCluster)
		err = r.setStatus(cr, api.RestoreStartCluster, "")
		if err != nil {
			err = errors.Wrap(err, "set status")
			return rr, err
		}
	}

	err = k8s.UnpauseClusterWithWait(ctx, r.client, clusterOrig)
	if err != nil {
		err = errors.Wrap(err, "restart cluster")
		return rr, err
	}

	log.Info(returnMsg)

	return rr, err
}

func (r *ReconcilePerconaXtraDBClusterRestore) getBackup(ctx context.Context, cr *api.PerconaXtraDBClusterRestore) (*api.PerconaXtraDBClusterBackup, error) {
	if cr.Spec.BackupSource != nil {
		status := cr.Spec.BackupSource.DeepCopy()
		status.State = api.BackupSucceeded
		status.CompletedAt = nil
		status.LastScheduled = nil
		return &api.PerconaXtraDBClusterBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cr.Name,
				Namespace: cr.Namespace,
			},
			Spec: api.PXCBackupSpec{
				PXCCluster:  cr.Spec.PXCCluster,
				StorageName: cr.Spec.BackupSource.StorageName,
			},
			Status: *status,
		}, nil
	}

	bcp := &api.PerconaXtraDBClusterBackup{}
	err := r.client.Get(ctx, types.NamespacedName{Name: cr.Spec.BackupName, Namespace: cr.Namespace}, bcp)
	if err != nil {
		err = errors.Wrapf(err, "get backup %s", cr.Spec.BackupName)
		return bcp, err
	}
	if bcp.Status.State != api.BackupSucceeded {
		err = errors.Errorf("backup %s didn't finished yet, current state: %s", bcp.Name, bcp.Status.State)
		return bcp, err
	}

	return bcp, nil
}

const backupRestoredMsg = `You can view xtrabackup log:
$ kubectl logs job/restore-job-%s-%s
If everything is fine, you can cleanup the job:
$ kubectl delete pxc-restore/%s
`

const waitLimitSec int64 = 300

func (r *ReconcilePerconaXtraDBClusterRestore) waitForPodsShutdown(ls map[string]string, namespace string, gracePeriodSec int64) error {
	for i := int64(0); i < waitLimitSec+gracePeriodSec; i++ {
		pods := corev1.PodList{}

		err := r.client.List(
			context.TODO(),
			&pods,
			&client.ListOptions{
				Namespace:     namespace,
				LabelSelector: labels.SelectorFromSet(ls),
			},
		)
		if err != nil {
			return errors.Wrap(err, "get pods list")
		}

		if len(pods.Items) == 0 {
			return nil
		}

		time.Sleep(time.Second * 1)
	}

	return errors.Errorf("exceeded wait limit")
}

func (r *ReconcilePerconaXtraDBClusterRestore) waitForPVCShutdown(ls map[string]string, namespace string) error {
	for i := int64(0); i < waitLimitSec; i++ {
		pvcs := corev1.PersistentVolumeClaimList{}

		err := r.client.List(
			context.TODO(),
			&pvcs,
			&client.ListOptions{
				Namespace:     namespace,
				LabelSelector: labels.SelectorFromSet(ls),
			},
		)
		if err != nil {
			return errors.Wrap(err, "get pvc list")
		}

		if len(pvcs.Items) == 1 {
			return nil
		}

		time.Sleep(time.Second * 1)
	}

	return errors.Errorf("exceeded wait limit")
}

func (r *ReconcilePerconaXtraDBClusterRestore) setStatus(cr *api.PerconaXtraDBClusterRestore, state api.BcpRestoreStates, comments string) error {
	cr.Status.State = state
	switch state {
	case api.RestoreSucceeded:
		tm := metav1.NewTime(time.Now())
		cr.Status.CompletedAt = &tm
	}

	cr.Status.Comments = comments

	err := r.client.Status().Update(context.TODO(), cr)
	if err != nil {
		return errors.Wrap(err, "send update")
	}

	return nil
}
