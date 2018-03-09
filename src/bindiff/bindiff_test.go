package bindiff

import (
	"flag"
	"testing"
)

var filename = flag.String("f", "", "test file name")
var datapath = flag.String("p", "", "test file path")

func TestCreatePatch(t *testing.T) {
	
	t.Logf("Current path is %s, file name is %s", *datapath, *filename)
	
	err := CreatePatch(*datapath + "/" + *filename)
	if err != nil {
		t.Errorf("Create patch for %s failed: %s", filename, err.Error())
	}
}

func TestGetMetaData(t *testing.T) {
	
	t.Logf("Current path is %s, file name is %s", *datapath, *filename)	
	
	filepath := *datapath + "/" + *filename
	
	err := CreatePatch(filepath)
	if err != nil {
		t.Errorf("Create patch for %s failed: %s", filename, err.Error())
	}
	
	var metadata FileMetaData

	if metadata, err = GetFileMetaData(filepath); err != nil {
		t.Errorf("Fail to get meta data for %s: %s", filename, err.Error())
	}
	
	t.Log(metadata)
	
	t.Logf("prev file hash is %x", metadata.PrevPatchHash)
	t.Logf("file hash is %x", metadata.PatchHash)
}

