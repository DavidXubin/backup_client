package s3

import (
	"testing"
	"flag"
	"encoding/hex"
	"../bindiff"
)

var filename = flag.String("f", "", "test file name")
var datapath = flag.String("p", "", "test file path")

func TestUploadObject(t *testing.T) {
		
	t.Logf("Current path is %s, file name is %s", *datapath, *filename)	
	
	filepath := *datapath + "/" + *filename
	bucket := "mozylab"
	
	s3Conveyor := NewS3Conveyor("us-east-2")
	
	if err := s3Conveyor.UploadObject(bucket, "test", filepath); err != nil {
		t.Errorf("Fail to upload test.txt: %s", err.Error())
	}
}


func TestDownloadObject(t *testing.T) {
		
	t.Logf("Current path is %s, file name is %s", *datapath, *filename)
	
	downloadfile := *datapath + "/download_" + *filename
	bucket := "mozylab"
	
	var metadata bindiff.FileMetaData
	var err error
	if metadata, err = bindiff.GetFileMetaData(*datapath + "/" + *filename); err != nil {
		t.Errorf("Fail to get meta data: %s", err.Error())
		return
	}
	
	s3Conveyor := NewS3Conveyor("us-east-2")
	patchHash := hex.EncodeToString(metadata.PatchHash[:])
	
	if err = s3Conveyor.DownloadObject(bucket, 
		      "test/" + patchHash[0:2] + "/" + patchHash[2:4] + "/" + patchHash + ".dat", downloadfile); err != nil {
		t.Errorf("Fail to download test.txt: %s", err.Error())
	}
}


func TestListBuckets(t *testing.T) {
	
	var buckets []string
	var err error
	
	s3Conveyor := NewS3Conveyor("us-east-2")
	
	if buckets, err = s3Conveyor.ListBuckets(); err != nil {
		t.Errorf("Fail to list buckets: %s", err.Error())
		return
	}
	
	for _, bucket := range buckets {
		t.Logf("bucket is %s", bucket)
	}
}


func TestCreatePathInBucket(t *testing.T) {
	
	s3Conveyor := NewS3Conveyor("us-east-2")
	
	if err := s3Conveyor.CreatePathInBucket("mozylab", "test/"); err != nil {
		t.Errorf("Fail to create test1/test2 in bucket: %s", err.Error())
		return		
	}	
}


func TestCheckPathInBucket(t *testing.T) {
	
	s3Conveyor := NewS3Conveyor("us-east-2")
	
	found := false
	var err error
	if found, err = s3Conveyor.CheckPathInBucket("mozylab", "test/"); err != nil {
		t.Error(err)
	}
	
	if found {
		t.Logf("Found test/ in bucket: mozylab")
	} else {
		t.Logf("Failed to find test/ in bucket: %s")
	}
	
	if found, err = s3Conveyor.CheckPathInBucket("mozylab", "test3/"); err != nil {
		t.Error(err)
	}
	
	if found {
		t.Log("Found test3/ in bucket: mozylab")
	} else {
		t.Log("Failed to find test3/ in bucket: mozylab")
	}	
}

func TestGeneratePresignedURL(t *testing.T) { 
		
	bucket := "mozylab"
	
	t.Logf("Current path is %s, file name is %s", *datapath, *filename)	
	
	var metadata bindiff.FileMetaData
	var err error
	if metadata, err = bindiff.GetFileMetaData(*datapath + "/" + *filename); err != nil {
		t.Errorf("Fail to get meta data: %s", err.Error())
		return
	}	
	
	s3Conveyor := NewS3Conveyor("us-east-2")
		
	hash := make([]byte, len(metadata.PatchHash))
	copy(hash, metadata.PatchHash[:])
	object := hex.EncodeToString(hash)
	
	var url string
	if url, _, err = s3Conveyor.GeneratePresignedURL(bucket, "test", object); err != nil {
		t.Errorf("Fail to create presigned url for %s in bucket: %s", object, err.Error())
		return			
	}
	
	t.Logf("presigned url is %s", url)
}
