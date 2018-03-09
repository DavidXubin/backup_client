package triton

import (
	"os"
	"time"
	"strings"
	"strconv"
	"net/http"
	"net/url"
	"io/ioutil"
	"math/rand"
	"encoding/xml"
	"encoding/hex"
	"encoding/base64"
	"github.com/pkg/errors"
	
	"../bindiff"
	"../slog"
)


type Version struct {
	XMLName xml.Name  `xml:"Version"`
	Deleted string    `xml:"deleted,attr"`
	Type string       `xml:"type,attr"`
	Format string     `xml:"format,attr"`
	VersionId string  `xml:"versionId"`
	Size int64        `xml:"size"`
	PatchSize int64   `xml:"patchSize"`
	Stime string      `xml:"stime"`
	Mtime string      `xml:"mtime"`
	Ctime string      `xml:"ctime"`
	ObjectId string   `xml:"objectId"`
	Ordinal int64     `xml:"monotonicOrdinal"`
}

type VersionList struct {
	Versions []Version `xml:"Version"`
}

type NamedObject struct {
	XMLName xml.Name     `xml:"NamedObject"`
	Deleted string       `xml:"deleted,attr"`
	Type string          `xml:"version,attr"`
	Id string    	     `xml:"id"`
	VersionId string     `xml:"versionId"`
	Fullpath string      `xml:"fullpath"`
	Versions VersionList `xml:"VersionList"`
}


type ObjectList struct {
	XMLName xml.Name            `xml:"ObjectList"`
	NamedObjects []NamedObject  `xml:"NamedObject"`
}

type ListNamedObjectResults struct {
	XMLName xml.Name   `xml:"ListNamedObjectResults"`
	Objects ObjectList `xml:"ObjectList"`
}

func init() {
	err := slog.SetSyslog("triton_conveyor")
	if err != nil {
		slog.Error(err)
	}
}


func basicAuth(username, password string) string {
  	auth := username + ":" + password
  	return base64.StdEncoding.EncodeToString([]byte(auth))
}


type TritonClient struct {
	Name string
	Passwd string
	Container string
}

type TritonConveyor struct {
	Account TritonClient
	Endpoints []string
}

func NewTritonConveyor() *TritonConveyor {
			
	return &TritonConveyor{Account: TritonClient{},  Endpoints: []string{}}
}


func (conveyor *TritonConveyor) SetAccount(name string, passwd string, container string) (client TritonClient, err error) {
	
	client = TritonClient{Name: name, Passwd: passwd, Container: container}
	conveyor.Account = client
	
	return
}

func (conveyor *TritonConveyor) AddEndpoints(tdses []string) (err error) {
	
	if len(tdses) > 0 {
		conveyor.Endpoints = append(conveyor.Endpoints, tdses...)
	}
	return
}


func (conveyor *TritonConveyor) PickEndpoint() (endpoint string, err error) {
	
	if len(conveyor.Endpoints) == 0 {
		err = errors.New("No tds added")
		return		
	}
	
	 rand.Seed(time.Now().Unix())
	 randNum := rand.Intn(len(conveyor.Endpoints))
	 endpoint = conveyor.Endpoints[randNum]
	 
	 return
}  


func (conveyor *TritonConveyor) PostNamedObjects(filepath string, presignedURL string, xmeta map[string] string)  (err error) {
	
	var tds string
	if tds, err = conveyor.PickEndpoint(); err != nil {
		slog.Error(err)
		return
	} 

	var metadata bindiff.FileMetaData
	if metadata, err = bindiff.GetFileMetaData(filepath); err != nil {
		slog.Errorf("Fail to get meta data for %s: %s", filepath, err.Error())
		return
	}
	
	postURL := "http://" + tds + "/namedObjects/" + conveyor.Account.Container + "/" + url.QueryEscape(filepath) + "?presignedURL=" + presignedURL

	var req *http.Request	
	var rangefile *os.File

	if metadata.PatchType == bindiff.FORMAT_BASELINE {
		
		if req, err = http.NewRequest("POST", postURL, nil); err != nil {
			slog.Error(err)
			return
		}
		
		req.Header.Set("X-Eventual-Content-Length", strconv.FormatInt(metadata.FileSize, 10))
	} else {
		
		if rangefile, err = os.Open(bindiff.S3_TRITON_ROOT + filepath + ".range"); err != nil {
			slog.Error(err)
			return
		}
		defer rangefile.Close()
		
		if req, err = http.NewRequest("POST", postURL, rangefile); err != nil {
			slog.Error(err)
			return
		}
		
		req.Header.Set("X-Triton-Legacy-Patch-Headers", "true")
		req.Header.Set("X-Eventual-Patch-Length", strconv.FormatInt(metadata.PatchSize, 10))
		
		hash := make([]byte, len(metadata.PrevPatchHash))
		copy(hash, metadata.PrevPatchHash[:])
		req.Header.Set("X-Previous-Objectid", hex.EncodeToString(hash))
	}
	
	req.Header.Set("Authorization", "Basic " + basicAuth(conveyor.Account.Name, conveyor.Account.Passwd))
	
	hash := make([]byte, len(metadata.PatchHash))
	copy(hash, metadata.PatchHash[:])
	req.Header.Set("X-Objectid", hex.EncodeToString(hash))
	req.Header.Set("X-Triton-No-Encrypt", "yes")
	
	if len(xmeta) > 0 {
		var xmetaStr string
		for k, v := range xmeta {
			xmetaStr = xmetaStr + k + "=" + v + ","
		}
		
		req.Header.Set("X-Meta", xmetaStr[: len(xmetaStr) - 1])
	}
	
	var resp *http.Response 

	if resp, err = http.DefaultClient.Do(req); err != nil {
		slog.Errorf("error sending request %s: %s", postURL, err.Error())
		return
	}
	defer resp.Body.Close()

	slog.Infoln(resp.Status)
	slog.Infoln(resp.Header)

	var body []byte
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		slog.Errorf("error getting response %s: %s", postURL, err.Error())
		return		
	}
	slog.Infoln(string(body))
	
	if strings.Contains(resp.Status, "200") || strings.Contains(resp.Status, "201") {
		return
	} else {
		err = errors.New(string(body))
		return
	}	
}

func (conveyor *TritonConveyor) ListNamedObjects(result *ListNamedObjectResults, filepath string)  (err error) {
	
	var tds string
	if tds, err = conveyor.PickEndpoint(); err != nil {
		slog.Error(err)
		return
	} 
	
	listURL := "http://" + tds + "/namedObjects/" + conveyor.Account.Container + "/?FullPath=" + url.QueryEscape(filepath) + 
	           "&includeObjectId=1&ReverseVersionOrder=1"
	
	var req *http.Request
	
	if req, err = http.NewRequest("GET", listURL, nil); err != nil {
		slog.Error(err)
		return
	}
	
	req.Header.Set("Authorization", "Basic " + basicAuth(conveyor.Account.Name, conveyor.Account.Passwd))
	
	var resp *http.Response 

	if resp, err = http.DefaultClient.Do(req); err != nil {
		slog.Errorf("error sending request %s: %s", listURL, err.Error())
		return
	}
	defer resp.Body.Close()

	slog.Infoln(resp.Status)
	slog.Infoln(resp.Header)

	var body []byte
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		slog.Errorf("error getting response %s: %s", listURL, err.Error())
		return		
	}
	
	err = xml.Unmarshal(body, result)
	if err != nil {
	    slog.Errorf("Parse xml error: %s", err.Error())
	    return
	}
	slog.Infoln(result)
	
	return	
}

