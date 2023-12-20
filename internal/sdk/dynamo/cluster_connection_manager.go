package dynamo

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ConnectionManager contains different cluster information and connection to them
type ConnectionManager struct {
	clusters map[string]*ClusterObject
	lock     sync.Mutex
}

// ConfigConnectionManager returns an instance of ConnectionManager
func ConfigConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		clusters: make(map[string]*ClusterObject),
		lock:     sync.Mutex{},
	}
}

// DisconnectAll disconnect all the clusters used in a tasks.Request
func (cm *ConnectionManager) DisconnectAll() {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	for cS := range cm.clusters {
		delete(cm.clusters, cS)
	}
}

// setClientObject maps a dynamodb cluster via region string to *DynamoDB
func (cm *ConnectionManager) setClientObject(clusterIdentifier string, c *ClusterObject) {
	cm.clusters[clusterIdentifier] = c
}

func (cm *ConnectionManager) getDynamoDBObject(clusterConfig *ClusterConfig) (*ClusterObject, error) {
	if clusterConfig == nil {
		return nil, fmt.Errorf("unable to parse clusterConfig | %w", errors.New("clusterConfig is nil"))
	}

	clusterIdentifier := clusterConfig.Region
	_, ok := cm.clusters[clusterIdentifier]
	if !ok {
		if err := ValidateClusterConfig(clusterConfig); err != nil {
			return nil, err
		}
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(clusterConfig.AccessKey, clusterConfig.SecretKeyId, "")),
			config.WithRegion(clusterConfig.Region),
		)
		if err != nil {

		}
		client := dynamodb.NewFromConfig(cfg)
		clusterObject := &ClusterObject{ClusterConfig: clusterConfig, Client: client}
		cm.setClientObject(clusterIdentifier, clusterObject)
	}
	return cm.clusters[clusterIdentifier], nil
}

func (cm *ConnectionManager) GetCluster(clusterConfig *ClusterConfig) (*ClusterObject, error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	cObj, err1 := cm.getDynamoDBObject(clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	return cObj, nil
}
