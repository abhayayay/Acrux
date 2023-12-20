package dynamo

import (
	"Acrux/internal/task_errors"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const WaitUnityReadyTime = 30
const WaitUnitlReadyTimeRetries = 5

type ClusterConfig struct {
	AccessKey   string           `json:"accessKey" doc:"true"`
	SecretKeyId string           `json:"secretKeyId" doc:"true"`
	Region      string           `json:"region" doc:"true"`
	Table       string           `json:"table,omitempty" doc:"true"`
	Client      *dynamodb.Client `json:"-"`
	DbType      string           `json:"dbType" doc:"true"`
}

type ClusterObject struct {
	ClusterConfig *ClusterConfig   `json:"cluster_config"`
	Client        *dynamodb.Client `json:"-"`
}

func ValidateClusterConfig(c *ClusterConfig) error {
	if c == nil {
		return task_errors.ErrParsingClusterConfig
	}
	if c.Region == "" || c.AccessKey == "" || c.SecretKeyId == "" {

		return task_errors.ErrCredentialMissing
	}
	return nil
}
