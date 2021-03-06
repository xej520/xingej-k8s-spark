package v1beta1

import (
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"fmt"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
)

type ClusterPhase string

type OperatorPhase string

type ClusterConditionType string

type SparkRole string

type RestartActionPhase string

const (
	ClusterPhaseRunning ClusterPhase = "Running"
	ClusterPhaseStopped ClusterPhase = "Stopped"
	ClusterPhaseFailed  ClusterPhase = "Failed"
	ClusterPhaseWaiting ClusterPhase = "Waiting"
	ClusterPhaseWarning ClusterPhase = "Warning"

	ServerRunning   v1.PodPhase = "Running"
	ServerStopped   v1.PodPhase = "Stopped"
	ServerFailed    v1.PodPhase = "Failed"
	ServerWaiting   v1.PodPhase = "Waiting"
	ServerPending   v1.PodPhase = "Pending"
	ServerUnknown   v1.PodPhase = "Unknown"
	ServerSucceeded v1.PodPhase = "Succeeded"
	ServerDeleted   v1.PodPhase = "Deleted"

	NodeOperatorPhaseStop   OperatorPhase = "NodeStop"
	NodeOperatorPhaseStart  OperatorPhase = "NodeStart"
	NodeOperatorPhaseDelete OperatorPhase = "NodeDelete"

	ClusterOperatorPhaseStop           OperatorPhase = "ClusterStop"
	ClusterOperatorPhaseStart          OperatorPhase = "ClusterStart"
	ClusterOperatorPhaseCreate         OperatorPhase = "ClusterCreate"
	ClusterOperatorPhaseAddNode        OperatorPhase = "AddNode"
	ClusterOperatorPhaseChangeResource OperatorPhase = "ChangeResource"
	ClusterOperatorPhaseChangeConfig   OperatorPhase = "ChangeConfig"
	ClusterOperatorPhaseDelete         OperatorPhase = "ClusterDelete"

	ClusterConditionInit          ClusterConditionType = "Init"
	ClusterConditionReady         ClusterConditionType = "Ready"
	ClusterConditionRecovering    ClusterConditionType = "Recovering"
	ClusterConditionScaling       ClusterConditionType = "Scaling"
	ClusterConditionUpgrading     ClusterConditionType = "Upgrading"
	ClusterConditionUnschedulable ClusterConditionType = "Unschedulable"

	SparkRoleMaster SparkRole = "master"
	SparkRoleSlave  SparkRole = "slave"

	DefaultSparkVersion = "2.1.0"

	RestartActionNo      RestartActionPhase = "No"
	RestartActionNeed    RestartActionPhase = "Need"
	RestartActionStarted RestartActionPhase = "Started"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SparkCluster is a spark cluster metadata
type SparkCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterSpec   `json:"spec"`
	Status ClusterStatus `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SparkClusterList is a list of spark clusters.
type SparkClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []SparkCluster `json:"items"`
}

type ClusterSpec struct {
	Spec `json:",inline"`

	SparkOperator `json:"sparkoperator"`

	Version string       `json:"version"`
	Image   string       `json:"image,omitempty"`
	Config  *SparkConfig `json:"config,omitempty"`

	HealthCheck  bool              `json:"healthcheck"`
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// The scheduling constraints on spark pods.
	Affinity *v1.Affinity `json:"affinity,omitempty"`
}

type ClusterStatus struct {
	ClusterPhase ClusterPhase `json:"phase"`

	ResourceUpdateNeedRestart  bool `json:"resourceupdateneedrestart"`
	ParameterUpdateNeedRestart bool `json:"parameterupdateneedrestart"`

	ServerNodes map[string]*Server `json:"serverNodes,omitempty"`

	// Represents the latest available observations of a cluster object's current state.
	Conditions                         []*ClusterCondition `json:"conditions,omitempty"`
	Reason                             string              `json:"reason,omitempty"`
	WaitSparkComponentAvailableTimeout int                 `json:"waitsparktimeout,omitempty"`
}

type Spec struct {
	Resources Resources `json:"resources"`
	Replicas  int       `json:"replicas,omitempty"`
	Volume    string    `json:"volume,omitempty"`
	Mount     string    `json:"volumeMount,omitempty"`
	Capactity string    `json:"capacity,omitempty"`
}

type SparkOperator struct {
	Node     string        `json:"nodename"`
	Operator OperatorPhase `json:"operator"`
}

type SparkConfig struct {
	Sparkcnf                  map[string]string `json:"sparkcnf,omitempty"`
	LivenessDelayTimeout      int               `json:"livenessDelayTimeout,omitempty"`
	ReadinessDelayTimeout     int               `json:"readinessDelayTimeout,omitempty"`
	LivenessFailureThreshold  int               `json:"livenessFailureThreshold,omitempty"`
	ReadinessFailureThreshold int               `json:"readinessFailureThreshold,omitempty"`
}

// Resources spec resource
type Resources struct {
	Requests MemoryCPU `json:"requests,omitempty"`
	Limits   MemoryCPU `json:"limits,omitempty"`
}

// MemoryCPU spec cpu and mem
type MemoryCPU struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

type Server struct {
	ID       string    `json:"id,omitempty"`
	Role     SparkRole `json:"role,omitempty"`
	Name     string    `json:"name,omitempty"`
	Nodeport int32     `json:"nodeport,omitempty"`
	Svcname  string    `json:"svcname,omitempty"`

	Configmapname string      `json:"configmapname,omitempty"`
	Address       string      `json:"address,omitempty"`
	Node          string      `json:"nodeName,omitempty"`
	Status  v1.PodPhase `json:"status,omitempty"`

	DownTime int64 `json:"downTime,omitempty"`

	RestartAction RestartActionPhase `json:"restartaction,omitempty"`
	VolumeID      string             `json:"volumeid,omitempty"`
	Operator      OperatorPhase      `json:"operator,omitempty"`
	NodeConfig    map[string]string  `json:"nodeconfig,omitempty"`
}

type ClusterCondition struct {

	// Type of cluster condition.
	Type ClusterConditionType `json:"type"`
	// Status of the condition, one of True, False, Unknown.
	Status v1.ConditionStatus `json:"status,omitempty"`
	// The last time this condition was updated.
	LastProbeTime metav1.Time `json:"lastUpdateTime,omitempty"`
	// Last time the condition transitioned from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
	// The reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// A human readable message indicating details about the transition.
	Message string `json:"message,omitempty"`

}
// GetKey get namespace/name
func (c *SparkCluster) GetKey() string {
	return fmt.Sprintf("%s/%s", c.Namespace, c.Name)
}

func (c *SparkCluster) SetDefaults()  {
	e := &c.Spec
	if len(e.Version) == 0 {
		e.Version = DefaultSparkVersion
	}

	if c.Status.WaitSparkComponentAvailableTimeout == 0 {
		c.Status.WaitSparkComponentAvailableTimeout = 240
	}
	if c.Status.ServerNodes == nil {
		c.Status.ServerNodes = make(map[string]*Server)
		role := SparkRoleSlave
		for i := 0; i < c.Spec.Replicas; {
			// 初始化名字
			serverID := utilrand.String(5)

			// 设置i=0时，为master角色，其他都是slave角色
			if i == 0 {
				role = SparkRoleMaster
			}else {
				role = SparkRoleSlave
			}

			name := fmt.Sprintf("spark-%s-%s-%s", c.Name, role, serverID)
			svcname := fmt.Sprintf("spark-%s-%s-%s", c.Name, role, serverID)
			configmapname := fmt.Sprintf("spark-config-%s-%s-%s", c.Name, role, serverID)
			volumeid := fmt.Sprintf("spark-%s-%s-%s-%v", c.Namespace, c.Name, role, serverID)

			yes := true
			for _, servernode := range c.Status.ServerNodes {
				if serverID == servernode.ID {
					yes = false
					break
				}
			}
			if yes {
				c.Status.ServerNodes[name] = &Server{ID: serverID, Name: name, Svcname: svcname, Configmapname: configmapname,
					VolumeID: volumeid, Role: role, Status: ServerWaiting, RestartAction: RestartActionNo,
				}
				i++
			}
		}
	}

	c.Status.ClusterPhase = ClusterPhaseWaiting

	if len(c.Namespace) == 0 {
		c.Namespace = "default"
	}
	if e.HealthCheck {
		if e.Config.LivenessDelayTimeout <= 0 {
			e.Config.LivenessDelayTimeout = 30
		}
		if e.Config.LivenessFailureThreshold <= 0 {
			e.Config.LivenessFailureThreshold = 10
		}
		if e.Config.ReadinessDelayTimeout <= 0 {
			e.Config.ReadinessDelayTimeout = 30
		}
		if e.Config.ReadinessFailureThreshold <= 0 {
			e.Config.ReadinessFailureThreshold = 10
		}
	}
}

