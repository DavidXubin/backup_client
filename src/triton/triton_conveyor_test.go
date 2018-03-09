package triton

import (
	"testing"
	"flag"
	"encoding/hex"
	"../bindiff"
	"../s3"
)

var filename = flag.String("f", "", "test file name")
var datapath = flag.String("p", "", "test file path")

func TestPostNamedObjects(t *testing.T) { 
		
	t.Logf("Current path is %s, file name is %s", *datapath, *filename)
	filepath := *datapath + "/" + *filename
	
	var err error
	
	tritonConveyor := NewTritonConveyor()
	if _, err = tritonConveyor.SetAccount("David.X.Xu@emc.com", "123456", "4097"); err != nil {
		t.Errorf("Fail to create account", err.Error())
		return
	}
	
	tritonConveyor.AddEndpoints([]string{"172.16.31.68"})
	
	var metadata bindiff.FileMetaData
	if metadata, err = bindiff.GetFileMetaData(filepath); err != nil {
		t.Errorf("Fail to get meta data: %s", err.Error())
		return
	}	
			
	hash := make([]byte, len(metadata.PatchHash))
	copy(hash, metadata.PatchHash[:])
	object := hex.EncodeToString(hash)
	
	bucket := "mozylab"
	
	s3Conveyor := s3.NewS3Conveyor("us-east-2")
	
	var url, etag string
	if url, etag, err = s3Conveyor.GeneratePresignedURL(bucket, "test", object); err != nil {
		t.Errorf("Fail to create presigned url for %s in bucket: %s", object, err.Error())
		return			
	}
	
	t.Logf("presigned url is %s", url)
	t.Logf("ETag is %s", etag)
	
	if err = tritonConveyor.PostNamedObjects(filepath, url, map[string] string{"ETag" : etag}); err != nil {
		t.Errorf("Fail to post namedObjects: %s", err.Error())
		return			
	}
}


func TestListNamedObjects(t *testing.T) { 
		
	t.Logf("Current path is %s, file name is %s", *datapath, *filename)
	filepath := *datapath + "/" + *filename
	
	var err error
	
	tritonConveyor := NewTritonConveyor()
	if _, err = tritonConveyor.SetAccount("David.X.Xu@emc.com", "123456", "4097"); err != nil {
		t.Errorf("Fail to create account", err.Error())
		return
	}
	
	tritonConveyor.AddEndpoints([]string{"172.16.31.68"})
	
	var result ListNamedObjectResults
	
	if err = tritonConveyor.ListNamedObjects(&result, filepath); err != nil {
		t.Errorf("Fail to list namedObjects: %s", err.Error())
		return			
	}	
	
	t.Log(result)
}
