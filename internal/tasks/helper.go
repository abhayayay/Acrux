package tasks

import "Acrux/internal/task_errors"

const (
	MaxConcurrentRoutines                 = 128
	DefaultIdentifierToken                = "default"
	MaxQueryRuntime                int    = 86400
	DefaultQueryRunTime            int    = 100
	WatchIndexDuration             int    = 120
	InsertOperation                string = "insert"
	QueryOperation                 string = "query"
	DeleteOperation                string = "delete"
	UpsertOperation                string = "upsert"
	ReadOperation                  string = "read"
	TouchOperation                 string = "touch"
	ValidateOperation              string = "validate"
	SingleInsertOperation          string = "singleInsert"
	SingleDeleteOperation          string = "singleDelete"
	SingleUpsertOperation          string = "singleUpsert"
	SingleReadOperation            string = "singleRead"
	SingleTouchOperation           string = "singleTouch"
	SingleReplaceOperation         string = "singleReplace"
	CreatePrimaryIndex             string = "createPrimaryIndex"
	CreateIndex                    string = "createIndex"
	BuildIndex                     string = "buildIndex"
	RetryExceptionOperation        string = "retryException"
	SubDocInsertOperation          string = "subDocInsert"
	SubDocDeleteOperation          string = "subDocDelete"
	SubDocUpsertOperation          string = "subDocUpsert"
	SubDocReadOperation            string = "subDocRead"
	SubDocReplaceOperation         string = "subDocReplace"
	SingleSubDocInsertOperation    string = "singleSubDocInsert"
	SingleSubDocUpsertOperation    string = "singleSubDocUpsert"
	SingleSubDocReplaceOperation   string = "singleSubDocReplace"
	SingleSubDocDeleteOperation    string = "singleSubDocDelete"
	SingleSubDocReadOperation      string = "singleSubDocRead"
	SingleSubDocIncrementOperation string = "singleSubDocReadIncrement"
	SingleDocValidateOperation     string = "SingleDocValidate"
)

type Exceptions struct {
	IgnoreExceptions []string `json:"ignoreExceptions,omitempty" doc:"true"`
	RetryExceptions  []string `json:"retryExceptions,omitempty" doc:"true"`
	RetryAttempts    int      `json:"retryAttempts,omitempty" doc:"true"`
}

type RetriedResult struct {
	Status   bool   `json:"status" doc:"true"`
	CAS      uint64 `json:"cas" doc:"true"`
	InitTime string `json:"initTime" doc:"true"`
	AckTime  string `json:"ackTime" doc:"true"`
}

type OperationConfig struct {
	Count            int64      `json:"count,omitempty" doc:"true"`
	DocSize          int        `json:"docSize" doc:"true"`
	DocType          string     `json:"docType,omitempty" doc:"true"`
	KeySize          int        `json:"keySize,omitempty" doc:"true"`
	KeyPrefix        string     `json:"keyPrefix" doc:"true"`
	KeySuffix        string     `json:"keySuffix" doc:"true"`
	ReadYourOwnWrite bool       `json:"readYourOwnWrite,omitempty" doc:"true"`
	TemplateName     string     `json:"template" doc:"true"`
	Start            int64      `json:"start" doc:"true"`
	End              int64      `json:"end" doc:"true"`
	FieldsToChange   []string   `json:"fieldsToChange" doc:"true"`
	Exceptions       Exceptions `json:"exceptions,omitempty" doc:"true"`
}

type SingleOperationConfig struct {
	Keys     []string `json:"keys" doc:"true"`
	Template string   `json:"template" doc:"true"`
	DocSize  int      `json:"docSize" doc:"true"`
}

func configInsertOptions(i *InsertOptions) error {
	if i == nil {
		return task_errors.ErrParsingInsertOptions
	}
	if i.Timeout == 0 {
		i.Timeout = 10
	}
	return nil
}

func configSingleOperationConfig(s *SingleOperationConfig) error {
	if s == nil {
		return task_errors.ErrParsingSingleOperationConfig
	}
	return nil
}

type InsertOptions struct {
	Expiry      int64  `json:"expiry,omitempty" doc:"true"`
	PersistTo   uint   `json:"persistTo,omitempty" doc:"true"`
	ReplicateTo uint   `json:"replicateTo,omitempty" doc:"true"`
	Durability  string `json:"durability,omitempty" doc:"true"`
	Timeout     int    `json:"timeout,omitempty" doc:"true"`
}
