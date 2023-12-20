package task_errors

import "errors"

var (
	ErrRequestIsNil                 = errors.New("request.Request struct is nil")
	ErrTaskStateIsNil               = errors.New("task State is nil")
	ErrParsingClusterConfig         = errors.New("unable to parse clusterConfig")
	ErrCredentialMissing            = errors.New("missing credentials for authentication")
	ErrParsingSingleOperationConfig = errors.New("unable to parse SingleOperationConfig")
	ErrAWSRegionMissing             = errors.New("missing aws region")
	ErrParsingInsertOptions         = errors.New("unable to parse InsertOptions")
)
