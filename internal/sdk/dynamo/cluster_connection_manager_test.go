package dynamo

import (
	"Acrux/internal/task_meta_data"
	"log"
	"testing"
)

func TestConfigConnectionManager(t *testing.T) {
	cConfig := &ClusterConfig{
		AccessKey:   "",
		SecretKeyId: "",
		Region:      "",
		Table:       "docLoaderTest",
		Client:      nil,
		DbType:      "Dynamo",
	}

	cmObj := ConfigConnectionManager()

	if _, err := cmObj.GetCluster(cConfig); err != nil {
		log.Println(err)
		t.Fail()
	} else {
		m := task_meta_data.NewMetaData()
		cm1 := m.GetCollectionMetadata("x", 255, 1024, "json", "", "", "person")

		cm2 := m.GetCollectionMetadata("x", 255, 1024, "json", "", "", "person")

		if cm1.Seed != cm2.Seed {
			t.Fail()
		}

	}
}
