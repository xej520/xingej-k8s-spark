package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiv1 "k8s.io/api/core/v1"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:defaulter-gen=true

type SparkApplication struct {
	metav1.TypeMeta               `json:",inline"`
	metav1.ObjectMeta             `json:"metadata"`
	Spec   SparkApplicationSpec   `json:"spec"`
	Status SparkApplicationStatus `json:"status,omitempty"`
}
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type SparkApplicationList struct {
	metav1.TypeMeta          `json:",inline"`
	metav1.ListMeta        `json:"metadata,omitempty"`
	Items []SparkApplication `json:"items,omitempty"`
}

// 表示SparkApplication的部署模式
// 也就是说App运行时，driver进程的启动位置是在什么地方？
// 是在Spark集群里，还是在当前提交App的客户机上，
// 或者说，是client模式 还是 cluster模式
type DeployMode string

// 表示当前Spark应用程序是什么类型的语言，如java，python，scala，r
type SparkApplicationType string

const (
	JavaAppType   SparkApplicationType = "Java"
	PythonAppType SparkApplicationType = "Python"
	RAppType      SparkApplicationType = "R"
	ScalaAppType  SparkApplicationType = "Scala"
)


// Spec是对SparkApplication注册的对象的详细描述的主体部分
// 会被k8s持久化到etcd中保存的，
// 系统通过Spec的描述来创建或者更新对象
// 以达到用户期望的对象运行状态
// 这里主要提供的属性包括：配置设置，默认值，属性的初始化值
// 也包括在对象创建过程中由其他相关组件例如schedulers, auto-scalers创建或修改的对象属性，比如
// Pod的Service IP地址，如果spec被删除的话，那么该对象将会从系统中被删除
// 也可用说是Spark应用的规格
type SparkApplicationSpec struct {
	Type       SparkApplicationType `json:"type"`
	DeployMode DeployMode           `json: "deployMode"`

	// 非必填
	Image *string `json:"image,omitempty"`

	// 拉取镜像策略
	ImagePullPolicy *string `json:"imagePullPolicy, omitempty"`

	// spark app的主类
	MainClass *string `json:"mainClass,omitempty"`

	// 非必填
	// 表示main类所在的jar包位置
	// "local:///opt/spark/examples/jars/spark-examples_2.11-2.3.0.jar"
	MainApplicationFile *string `json:"mainApplicationFile"`

	// 非必填
	// 传递给应用程序的参数
	Arguments []string `json:"arguments,omitempty"`

	//	非必填
	Volumes []apiv1.Volume `json:"volumes,omitempty"`

	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty"`

	// 	driver的规格说明
	Driver DriverSpec `json:"driver"`

	//	executor的规格说明
	Executor ExecutorSpec `json:"executor"`

	// 节点选择
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// 提交应用的最大次数
	MaxSubmissionRetries *int32 `json:"maxSubmissionRetries,omitempty"`

	// spark应用的提交间隔
	SubmissionRetryInterval *int64 `json:"submissionRetryInterval,omitempty"`
}

// 主要是用来记录对象在系统中的当前状态信息，
// 表示的是运行状态的一些信息
type SparkApplicationStatus struct {
	//作为标签，添加到SparkApplication对象、driver、executor 的pod中
	AppID string `json:"appId,omitempty"`

	// 应用提交的时间
	SubmissionTime metav1.Time `json:"submissionTime,omitempty"`

	//应用运行完毕的时间
	CompletionTime metav1.Time `json:"completionTime,omitempty"`

	// 关于driver的一些信息
	DriverInfo DriverInfo `json:"driverInfo"`

	// 表明应用的整体状态，如此时这个应用处于什么状态，如new，submission, submission_failed,running,unkown
	AppState ApplicationState `json:"applicationState,omitempty"`

	// key 是 executor的Pod 名称
	// value是 当前执行器pod的状态，如 pending,running,COMPLETED,unkown,failed
	ExecutorState map[string]ExecutorState `json:"executorState,omitempty"`

	// 失败提交的最大次数
	SubmissionRetries int32 `json:"submissionRetries,omitempty"`
}

// 表示，当前executor的状态
type ExecutorState string

// 表示，应用的当前状态类型
type ApplicationStateType string

// 列出一个spark应用 可能存在的状态，如new，running，failed，submission_failed (提交失败)
const (
	NewState              ApplicationStateType = "NEW"
	SubmittedState        ApplicationStateType = "SUBMITTED"
	RunningState          ApplicationStateType = "RUNNING"
	CompletedState        ApplicationStateType = "COMPLETED"
	FailedState           ApplicationStateType = "FAILED"
	FailedSubmissionState ApplicationStateType = "SUBMISSION_FAILED"
	UnknownState          ApplicationStateType = "UNKNOWN"
)

// driver 一些描述信息
type DriverInfo struct {
	WebUIServiceName string `json:"webUIServiceName,omitempty"`
	WebUIPort        int32  `json:"webUIPort,omitempty"`
	WebUIAddress     string `json:"webUIAddress,omitempty"`
	PodName          string `json:"podName,omitempty"`
}

type ApplicationState struct {
	State        ApplicationStateType `json:"state"`
	ErrorMessage string               `json:"errorMessage"`
}

// 应用程序的重启策略
type RestartPolicy string

const (
	Undefined RestartPolicy = ""
	Never     RestartPolicy = "Never"
	OnFailure RestartPolicy = "OnFailure"
	Always    RestartPolicy = "Always"
)

type DriverSpec struct {
	SparkPodSpec

	// 选填
	// PodName 就是指用户创建的driver pod的名称
	PodName *string `json:"podName,omitempty"`
}

type ExecutorSpec struct {
	SparkPodSpec
	// 非必填
	// executor  的实例个数
	Instances *int32 `json:"instances,omitempty"`
}

type SparkPodSpec struct {
	// 非必填
	// 分配给Pod的cpu数量
	Cores *int32 `json:"cores,omitempty"`

	//	cpu限制
	//  非必填
	CoreLimit *string `coreLimit,omitempty"`

	//  非必填
	// 分配给pod的内存
	Memory *string `json:"memory,omitempty"`

	//	变量的设置
	EnvVars map[string]string `json:"memory,omitempty"`

	Lables map[string]string `json:"labels,omitempty"`

	Annoations map[string]string `json:"annotations,omitempty"`

	VolumeMounts []apiv1.VolumeMount `json:"volumeMounts,omitempty"`
}
