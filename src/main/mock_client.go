package main

import (
	"os"
	"fmt"
	"flag"
	"strings"
	"strconv"
	"io/ioutil"
	"encoding/xml"
	"encoding/hex"
	"github.com/pkg/errors"

	"../slog"
	"../bindiff"
	"../s3"
	"../triton"
)


type AccountSetting struct {
	XMLName xml.Name   	`xml:"AccountSetting"`
	Bucket string      	`xml:"Bucket"`
	Name string        	`xml:"Name"`
	Passwd string      	`xml:"Passwd"`
	MachineId string   	`xml:"MachineId"`
	DownloadBase string `xml:"DownloadBase"`
}


var tritonConveyor = triton.NewTritonConveyor()

var s3Conveyor = s3.NewS3Conveyor("us-east-2")

var listNamedObjects = triton.ListNamedObjectResults{}

var accountSetting = AccountSetting{} 

func ExitErrorf(msg string, args ...interface{}) {
    fmt.Fprintf(os.Stderr, msg + "\n", args...)
    os.Exit(1)
}


func ParseAccount(filepath string) (account AccountSetting, err error) {
	
	var accountfile *os.File
	if accountfile, err = os.Open(filepath); err != nil {
		slog.Errorf("Open %s failed: ", filepath, err.Error())
		return
	}
	defer accountfile.Close()
			
	var buf []byte
	if buf, err = ioutil.ReadAll(accountfile); err != nil {
		slog.Errorf("Read %s failed: %s", filepath, err.Error())
		return		
	}
	
	err = xml.Unmarshal(buf, &account)
	if err != nil {
	    slog.Errorf("Parse xml error: %s", err.Error())
	    return
	}
	
	slog.Infoln(account)
	return	
}


func ListFile(filepath string, fileVersions *[]triton.Version) (result string, err error) {
	
	var listResult triton.ListNamedObjectResults
	
	if err = tritonConveyor.ListNamedObjects(&listResult, filepath); err != nil {
		result = fmt.Sprintf("Fail to list named objects for %s: %s", filepath, err.Error())
	    slog.Error(result)
	    return		
	}
	
	namedObject := listResult.Objects.NamedObjects[0]
	
	if namedObject.Deleted != "false" {
		err = errors.New("Named object deleted")
		result = fmt.Sprintf("Named object is deleted for %s", filepath)
		return
	}
	
	idx := 1
	
	for _, version := range namedObject.Versions.Versions {
		if version.Deleted != "false" {
			continue
		}
		
		*fileVersions = append(*fileVersions, version)
			
		result += "Version index " + strconv.FormatInt(int64(idx), 10) + ":\n"
		result += "Store time: " + version.Stime + "\n"
		result += "Modify time: " + version.Mtime + "\n"
		result += "Create time: " + version.Ctime + "\n"
		result += "=======================================\n"
		
		idx += 1

	} 
	return
}


func GetFile(filepath string, idx int, fileVersions [] triton.Version) (err error) {
	
	lastSlash := strings.LastIndex(filepath, "/")
	tmpDownloadPath := bindiff.S3_TRITON_ROOT + filepath[:lastSlash] + "/tmp_download/"
	
	if err = bindiff.CreateDirIfNotExist(tmpDownloadPath); err != nil {
		slog.Errorf("Fail to create %s: %s", tmpDownloadPath, err.Error())
		return		
	}
	
	downloadFiles := []string{} 
	
	for _, version := range fileVersions[idx:] {
		found := false
		
		for _, file := range downloadFiles {
			if file == (tmpDownloadPath + version.ObjectId + ".dat") {
				found = true
				break
			}
		}
		
		if found == true {
			continue
		}
		
		if err = s3Conveyor.DownloadObject(accountSetting.Bucket, 
			                               accountSetting.MachineId + "/" + version.ObjectId[0:2] + "/" + version.ObjectId[2:4] + "/" + version.ObjectId + ".dat", 
			                               tmpDownloadPath + version.ObjectId + ".dat"); err != nil {
			slog.Errorf("Fail to download %s.dat: %s", version.ObjectId, err.Error())
			return
		}
		
		downloadFiles = append(downloadFiles, tmpDownloadPath + version.ObjectId + ".dat")	                               
	
		if version.Type == "baseline" {
			break
		}
	}
	
	i := 0
	j := len(downloadFiles) - 1
	
	for {	
		if i >= j {
			break
		}
		
		downloadFiles[i], downloadFiles[j] = downloadFiles[j], downloadFiles[i]
		
		i += 1
		j -= 1		
	}
	
	if err = bindiff.ConsolidatePatches(downloadFiles[0], downloadFiles[1:]); err != nil {
		slog.Errorf("Fail to consolidate file %s: %s", filepath, err.Error())
		return
	}
		
	if err = bindiff.CreateDirIfNotExist(accountSetting.DownloadBase + filepath[:lastSlash]); err != nil {
		slog.Errorf("Fail to create %s: %s", accountSetting.DownloadBase + filepath[:lastSlash], err.Error())
		return		
	}
	
	if err = os.Rename(downloadFiles[0], accountSetting.DownloadBase + filepath); err != nil {
		slog.Errorf("Fail to move %s to %s: %s", downloadFiles[0], accountSetting.DownloadBase + filepath, err.Error())
		return		
	}
	
	return
}

func PutFile(filepath string) (err error) {
	
	emptyAccount := AccountSetting{}
	
	if accountSetting == emptyAccount {
		err = errors.New("Account is missing")
		return
	}
	
	found := false
	if found, err = s3Conveyor.CheckPathInBucket(accountSetting.Bucket, accountSetting.MachineId); err != nil {
		slog.Error(err)
		return
	}
	
	if found == false {
		if err = s3Conveyor.CreatePathInBucket(accountSetting.Bucket, accountSetting.MachineId); err != nil {
			slog.Error(err)
			return		
		}	
	}
	
	if err = bindiff.CreatePatch(filepath); err != nil {
		slog.Errorf("Failed to create patch for %s: %s", filepath, err.Error())
		return		
	}
	
	if err = s3Conveyor.UploadObject(accountSetting.Bucket, accountSetting.MachineId, filepath); err != nil {
		slog.Errorf("Failed to upload %s: %s", filepath, err.Error())
		return	
	}
	
	var metadata bindiff.FileMetaData

	if metadata, err = bindiff.GetFileMetaData(filepath); err != nil {
		slog.Errorf("Fail to get meta data of %s: %s", filepath, err.Error())
		return
	}
			
	hash := make([]byte, len(metadata.PatchHash))
	copy(hash, metadata.PatchHash[:])
	object := hex.EncodeToString(hash)
	
	var url, etag string
	if url, etag, err = s3Conveyor.GeneratePresignedURL(accountSetting.Bucket, accountSetting.MachineId, object); err != nil {
		slog.Errorf("Fail to create presigned url for %s in bucket: %s", object, err.Error())
		return			
	}
	
	if err = tritonConveyor.PostNamedObjects(filepath, url, map[string] string{"ETag" : etag}); err != nil {
		slog.Errorf("Fail to post namedObjects: %s", err.Error())
		return			
	}	
	
	return
}


func main() {
	
	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    -f <file full path> -h <tds host> -a <account file path> -m <get / put>\n ")
		flag.PrintDefaults()
	}
	
	var filepath string
	var tds string
	var accountFile string
	var method string
	
	flag.StringVar(&accountFile, "a", "", "The account file full path")
	flag.StringVar(&filepath, "f", "", "The full file path to upload or download")
	flag.StringVar(&tds, "h", "172.16.31.68", "The trogdor host")
	flag.StringVar(&method, "m", "", "Whether get or put")
	
	flag.Parse()
	
	if len(os.Args) == 1 {
		flag.Usage()
		os.Exit(1)
	}
	
	var err error
	if accountSetting, err = ParseAccount(accountFile); err != nil {
		ExitErrorf("Parse account %s failed:\n", accountFile, err.Error())
	}
	
	if _, err = tritonConveyor.SetAccount(accountSetting.Name, accountSetting.Passwd, accountSetting.MachineId); err != nil {
		ExitErrorf("Get account %s failed:\n", accountFile, err.Error())
	}
	
	tritonConveyor.AddEndpoints([]string{tds})	
	
	switch method {
		case "get":
			var listResult string
			var fileVersions []triton.Version
			if listResult, err = ListFile(filepath, &fileVersions); err != nil {
				ExitErrorf("%s: %s", listResult, err.Error())
			}
			
			fmt.Print(listResult)
			fmt.Print("Please select a version index to restore:\n")
			
			var versionIdx int
			fmt.Scanf("%d", &versionIdx)
		
			if err = GetFile(filepath, versionIdx - 1, fileVersions); err != nil {
				ExitErrorf("Fail to get file of %s whose version is %s: %s", filepath, fileVersions[versionIdx - 1].VersionId, err.Error())
			}
		case "put":
			if err = PutFile(filepath); err != nil {
				ExitErrorf("Fail to put %s: %s", filepath, err.Error())
			}	
		default:
			ExitErrorf("Unsupport method: %s", method)
	}
}

