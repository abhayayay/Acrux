package tasks

import (
	"Acrux/internal/sdk/dynamo"
	"Acrux/internal/task_errors"
	"Acrux/internal/task_result"
	"Acrux/internal/template"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"reflect"
	"time"
)

type SingleInsertTask struct {
	IdentifierToken       string                  `json:"identifierToken" doc:"true"`
	ClusterConfig         *interface{}            `json:"clusterConfig" doc:"true"`
	InsertOptions         *InsertOptions          `json:"insertOptions,omitempty" doc:"true"`
	SingleOperationConfig *SingleOperationConfig  `json:"singleOperationConfig" doc:"true"`
	Operation             string                  `json:"operation" doc:"false"`
	ResultSeed            int64                   `json:"resultSeed" doc:"false"`
	TaskPending           bool                    `json:"taskPending" doc:"false"`
	Result                *task_result.TaskResult `json:"Result" doc:"false"`
	req                   *Request                `json:"-" doc:"false"`
}

func (task *SingleInsertTask) Describe() string {
	return "Single insert task create key value in Couchbase.\n"
}

func (task *SingleInsertTask) BuildIdentifier() string {
	if task.IdentifierToken == "" {
		task.IdentifierToken = DefaultIdentifierToken
	}
	return task.IdentifierToken
}

func (task *SingleInsertTask) CollectionIdentifier() string {
	if reflect.TypeOf(task.ClusterConfig) == reflect.TypeOf(&dynamo.ClusterConfig{}) {
		return task.IdentifierToken + task.ClusterConfig.Table
	}
	return "error"
}

func (task *SingleInsertTask) CheckIfPending() bool {
	return task.TaskPending
}

func (task *SingleInsertTask) Config(req *Request, reRun bool) (int64, error) {
	task.TaskPending = true
	task.req = req

	if task.req == nil {
		task.TaskPending = false
		return 0, task_errors.ErrRequestIsNil
	}

	task.req.ReconnectionManager(task.ClusterConfig.DbType)
	if _, err := task.req.connectionManager.GetCluster(task.ClusterConfig); err != nil {
		task.TaskPending = false
		return 0, err
	}

	task.req.ReconfigureDocumentManager()

	if !reRun {
		task.ResultSeed = int64(time.Now().UnixNano())
		task.Operation = SingleInsertOperation

		if err := configInsertOptions(task.InsertOptions); err != nil {
			task.TaskPending = false
			return 0, err
		}

		if err := configSingleOperationConfig(task.SingleOperationConfig); err != nil {
			task.TaskPending = false
			return 0, err
		}

	} else {
		log.Println("retrying :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
	}
	return task.ResultSeed, nil
}

func (task *SingleInsertTask) tearUp() error {
	task.Result.StopStoringResult()
	if err := task.Result.SaveResultIntoFile(); err != nil {
		log.Println("not able to save Result into ", task.ResultSeed)
	}
	task.Result = nil
	task.TaskPending = false
	return task.req.SaveRequestIntoFile()
}

func (task *SingleInsertTask) Do() error {

	task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)

	connectionObj, err1 := task.GetCollectionObject()

	if err1 != nil {
		task.Result.ErrorOther = err1.Error()
		task.Result.FailWholeSingleOperation(task.SingleOperationConfig.Keys, err1)
		return task.tearUp()
	}

	singleInsertDocuments(task, connectionObj)

	task.Result.Success = int64(len(task.SingleOperationConfig.Keys)) - task.Result.Failure
	return task.tearUp()
}

func singleInsertDocumentDynamo(task *SingleInsertTask, collectionObject *interface{}, initTime string, key string,
	doc interface{}, routineLimiter chan struct{}) error {
	input := &dynamodb.PutItemInput{
		TableName: collectionObject.ClusterConfig.Table,
		Item:      map[string]types.AttributeValue(doc),
	}
	_, err := collectionObject.client.PutItem(context.TODO(), input)
	if err != nil {
		task.Result.CreateSingleErrorResult(initTime, key, err.Error(), false, 0)
		<-routineLimiter
		log.Println("Error encountered while inserting :- ", err)
		return err
	}
	return nil
}

func singleInsertDocuments(task *SingleInsertTask, collectionObject *interface{}) {
	if task.req.ContextClosed() {
		return
	}

	routineLimiter := make(chan struct{}, MaxConcurrentRoutines)
	dataChannel := make(chan string, MaxConcurrentRoutines)

	group := error.Group{}
	for _, data := range task.SingleOperationConfig.Keys {
		if task.req.ContextClosed() {
			close(routineLimiter)
			close(dataChannel)
		}

		routineLimiter <- struct{}{}
		dataChannel <- data

		group.Go(func() error {
			key := <-dataChannel
			documentMetaData := task.req.DocumentsMeta.GetDocumentsMetadata(task.CollectionIdentifier(), key,
				task.SingleOperationConfig.Template,
				task.SingleOperationConfig.DocSize, false)

			fake := faker.NewWithSeed(rand.NewSource(int64(documentMetaData.Seed)))

			t := template.InitialiseTemplate(documentMetaData.Template)

			doc, _ := t.GenerateDocument(&fake, documentMetaData.DocSize)

			initTime := time.Now().UTC().Format(time.RFC850)
			if reflect.TypeOf(collectionObject) == reflect.TypeOf(&dynamo.ClusterObject{}) {
				result := singleInsertDocumentDynamo(task, collectionObject, initTime, key, doc, routineLimiter)
				if result != nil {
					return result
				}
			}

			return nil
		})
	}
	_ = group.Wait()
	close(routineLimiter)
	close(dataChannel)
	log.Println("completed :- ", task.Operation, task.BuildIdentifier(), task.ResultSeed)
}

func (task *SingleInsertTask) MatchResultSeed(resultSeed string) bool {
	if fmt.Sprintf("%d", task.ResultSeed) == resultSeed {
		if task.Result == nil {
			task.Result = task_result.ConfigTaskResult(task.Operation, task.ResultSeed)
		}
		return true
	}
	return false
}

func (task *SingleInsertTask) GetCollectionObject() (collectionObject interface{}, err error) {
	return task.req.connectionManager.GetCollection(task.ClusterConfig)
}

func (task *SingleInsertTask) SetException(exceptions Exceptions) {

}
