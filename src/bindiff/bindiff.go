package bindiff

import (
	"bufio"
	"os"
	"io"
	"strconv"
	"strings"
	"syscall"
	"math"
	"time"
	"unsafe"
	"io/ioutil"
	"crypto/sha1"
	"encoding/json"
	"encoding/hex"
	
	"../slog"
	"github.com/pkg/errors"
)

const (
	FORMAT_BASELINE  = 0
	FORMAT_PATCH = 1
)

const (
	PATCH_CHANGE = 0
	PATCH_TRUNCATE = 2
)

const RDIFF_BLOCKSIZE = 512 //4096

const S3_TRITON_ROOT string = "/opt/s3_triton/"

type SHAValue [20]byte

type Patch struct {
	Offset int64
	Size int64
	Type int8
}

type RDiffBlock struct {
	Offset int64
	Size int64
	Signature SHAValue
}

type FileMetaData struct {
	Backuptime int64       `json:"backuptime, string"`
	Atime int64            `json:"atime, string"`
	Mtime int64            `json:"mtime, string"`
	Ctime int64            `json:"ctime, string"`
	FileSize int64          `json:"file_size"`
	PatchSize int64         `json:"patch_size"`
	PatchType int8         `json:"patch_type"`
	PrevPatchHash SHAValue   `json:"prev_patch_hash,omitempty"`
	PatchHash SHAValue      `json:"patch_hash,omitempty"`
	PatchState []RDiffBlock `json:"patch_state,omitempty"`
}


func init() {
	err := slog.SetSyslog("bindiff")
	if err != nil {
		slog.Error(err)
	}
}


func (hash SHAValue) MarshalJSON() ([]byte, error) {
	var result string
	emptyHash := SHAValue{}
	if hash == emptyHash {
		result = `"` + `"`
	} else {
		result = `"` + hex.EncodeToString(hash[:]) + `"`
	}
	return []byte(result), nil	
}

func (hash *SHAValue) UnmarshalJSON(data []byte) error {
	
	if hash != nil {
		strHash := strings.Trim(string(data[:]), `"`)
		if len(strHash) == 0 {
			*hash = SHAValue{}
			return nil
		}
		
		if len(strHash) != 40 {
			return errors.New("Invalid hash")
		}
		
		var tmpHash []byte
		var err error
		if tmpHash, err = hex.DecodeString(strHash); err != nil {
			return err
		}
		
		copy((*hash)[:], tmpHash)
	}
	return nil
}

func (block RDiffBlock) MarshalJSON() ([]byte, error) {
     var result string
     if unsafe.Sizeof(block) == 0 {
         result = `"` + `"`
     } else {
         result = `"` + strconv.FormatInt(block.Offset, 10) + ":" + strconv.FormatInt(block.Size, 10) + ":" + 
				        hex.EncodeToString(block.Signature[:]) + `"`
     }
     return []byte(result), nil
 }

func (block *RDiffBlock) UnmarshalJSON(data []byte) error {
	var err error
	
	if block != nil {
		fields := strings.Split(strings.Trim(string(data[:]), `"`), ":")
		if block.Offset, err = strconv.ParseInt(fields[0], 10, 64); err != nil {
			return err
		}
		
		if block.Size, err = strconv.ParseInt(fields[1], 10, 64); err != nil {
			return err
		}
		 
		if len(fields[2]) != 40 {
			return errors.New("Invalid hash")			
		}
		
		var tmpHash []byte
		if tmpHash, err = hex.DecodeString(fields[2]); err != nil {
			return err
		}
		copy(block.Signature[:], tmpHash)  
	}
	
	return nil
}


func Minimum(first interface{}, rest ...interface{}) interface{}{
    minimum := first

    for _, v := range rest {
        switch v.(type) {
            case int:
                if v := v.(int); v < minimum.(int) {
                    minimum = v 
                }   
            case float64:
                if v := v.(float64); v < minimum.(float64) {
                    minimum = v 
                }   
            case string:
                if v := v.(string); v < minimum.(string) {
                    minimum = v 
                }   
        }   
    }   
    return minimum
}

func CreateDirIfNotExist(dir string) (err error) {
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0755); err != nil {
			return
		}
	}	
	return
}

func GetFileHash(filepath string) (hash []byte, err error) {
	var file *os.File
	
	if file, err = os.Open(filepath); err != nil {
	    return
	}
	defer file.Close()
	
	h := sha1.New()
	if _, err = io.Copy(h, file); err != nil {
	    return
	}	
	
	hash = h.Sum(nil)
	return
}

func MakeRDiffBlocks(filepath string) (rdiffBlocks []RDiffBlock, err error) {
	var file *os.File	
	if file, err = os.Open(filepath); err != nil {
		return
	}
	defer file.Close()
	
	bufReader := bufio.NewReader(file)
	buf := make([]byte, RDIFF_BLOCKSIZE)
	
	offset := 0
	h := sha1.New()
	for {
		var rc int
		rc, err = bufReader.Read(buf)
		if err != nil && err != io.EOF {
			return
		}
		
		if rc == 0 {
			break
		}
		
		h.Reset()
		if rc == RDIFF_BLOCKSIZE {
			h.Write(buf)
		} else {
			h.Write(buf[:rc])
		}
		
		var signature SHAValue
		copy(signature[:], h.Sum(nil))
		
		rdiffBlocks = append(rdiffBlocks, RDiffBlock{Offset: int64(offset), Size: int64(rc), Signature: signature})
		
		offset += rc
	}
	
	return
} 


func GetFileMetaData(filepath string) (metadata FileMetaData, err error) {
	
	metaFilePath := S3_TRITON_ROOT + filepath + ".meta"
	
	var fileStat syscall.Stat_t
	if err = syscall.Stat(metaFilePath, &fileStat); err != nil {
		return
	}
	
	var jsondata []byte
	if jsondata, err = ioutil.ReadFile(metaFilePath); err != nil {
		return
	}
	
	err = json.Unmarshal(jsondata, &metadata)	
	return
}

func IsBaseline(filepath string) (isBaseline bool) {
	
	lastSlash := strings.LastIndex(filepath, "/")
	fileMetaPath := S3_TRITON_ROOT + filepath[:lastSlash]
	
	if _, err := os.Stat(fileMetaPath); os.IsNotExist(err) {
		isBaseline = true
		return
	} 
	
	if _, err := os.Stat(S3_TRITON_ROOT + filepath + ".meta"); os.IsNotExist(err) {
		isBaseline = true
		return
	}		
	
	return
}

func UpdateFileMetaData(filepath string, state []RDiffBlock, prevPatchHash SHAValue, isBaseline bool)  (err error) {
	
	var fileStat syscall.Stat_t
	if err = syscall.Stat(filepath, &fileStat); err != nil {
		return
	}
	
	lastSlash := strings.LastIndex(filepath, "/")
	fileMetaPath := S3_TRITON_ROOT + filepath[:lastSlash]
	
	if err = CreateDirIfNotExist(fileMetaPath); err != nil {
		return
	}
		
	metaData := &FileMetaData{}	
	metaData.Atime = fileStat.Atim.Nano() / int64(math.Pow(10, 9))
	metaData.Mtime = fileStat.Mtim.Nano() / int64(math.Pow(10, 9))
	metaData.Ctime = fileStat.Ctim.Nano() / int64(math.Pow(10, 9))
	metaData.Backuptime = time.Now().Unix()
	metaData.FileSize = fileStat.Size
	
	patchFilePath := S3_TRITON_ROOT + filepath + ".patch"
	
	if isBaseline == false {
		metaData.PatchType = FORMAT_PATCH
					
		if err = syscall.Stat(patchFilePath, &fileStat); err != nil {
			return
		}
		
		metaData.PatchSize = fileStat.Size
		
		copy(metaData.PrevPatchHash[:], prevPatchHash[:])
		
		var hash []byte
		if hash, err = GetFileHash(patchFilePath); err != nil {
			return
		}
		copy(metaData.PatchHash[:], hash)
		
	} else {
		metaData.PatchType = FORMAT_BASELINE	
		metaData.PatchSize = metaData.FileSize
		
		var hash []byte
		if hash, err = GetFileHash(filepath); err != nil {
			return
		}
		copy(metaData.PatchHash[:], hash)
	}
	
	if state != nil && len(state) > 0 {
		metaData.PatchState = state
	} else {
		if metaData.PatchState, err = MakeRDiffBlocks(filepath); err != nil {
			return
		}			
	}
	
	var jsondata []byte
	if jsondata, err = json.Marshal(metaData); err != nil {
		return
	}
	
	err = ioutil.WriteFile(S3_TRITON_ROOT + filepath + ".meta", jsondata, 0666)
	return
}


func MergePatch(base string, patch string) (err error) {
	
	var fileStat syscall.Stat_t
	if err = syscall.Stat(base, &fileStat); err != nil {
		return
	}
	
	basefileSize := fileStat.Size
	
	var basefile *os.File	
	if basefile, err = os.OpenFile(base, os.O_WRONLY, 0666); err != nil {
		return
	}
	defer basefile.Close()
	
	var patchfile *os.File
	if patchfile, err = os.Open(patch); err != nil {
		return
	}
	defer patchfile.Close()
	
	patchFileReader := bufio.NewReader(patchfile)
	
	var strline string
	strline, err = patchFileReader.ReadString('\n') 
	if err != nil && err != io.EOF {
		return
	}
	
	patchlineCnt := int64(0) 
	if patchlineCnt, err= strconv.ParseInt(strline[:len(strline) - 1], 10, 64); err != nil {
		return
	}
	
	slog.Infof("patch line is %d\n", patchlineCnt)
	
	var patchlines []Patch
	
	for i := int64(0); i < patchlineCnt; i++ {
		strline, err = patchFileReader.ReadString('\n') 
		if err != nil && err != io.EOF {
			return
		}
		
		fields := strings.Split(strline[:len(strline) - 1], ":")
		
		offset := int64(0)
		if offset, err = strconv.ParseInt(fields[0], 10, 64); err != nil {
			return
		}
		
		size := int64(0)
		if size, err = strconv.ParseInt(fields[1], 10, 64); err != nil {
			return
		}
		
		patchType := int64(0)
		if patchType, err = strconv.ParseInt(fields[2], 10, 8); err != nil {
			return
		}
		
		patchlines = append(patchlines, Patch{Offset: offset, Size: size, Type: int8(patchType)})
	}
	
	slog.Infoln(patchlines)
	
	buf := make([]byte, RDIFF_BLOCKSIZE)
	rc := 0
	
	for i, patchline := range patchlines {
		
		if patchline.Size == 0 {
			err = basefile.Truncate(patchline.Offset)
			return
		}
		
		rc, err = patchFileReader.Read(buf)
		if err != nil && err != io.EOF {
			return
		}
		if int64(rc) != patchline.Size {
			slog.Errorf("failed at %d patch line, rc = %d, patch size = %d", i, rc, patchline.Size)
			err = errors.New("patchline size mismatched")
			return
		}
			
		if patchline.Offset < basefileSize {
			if rc, err = basefile.WriteAt(buf[:rc], patchline.Offset); err != nil {
				return
			}		
		} else {
			if err = basefile.Sync(); err != nil {
				return
			}
			
			if _, err = basefile.Seek(0, 2); err != nil {
				return
			}
			
			if rc, err = basefile.Write(buf[:rc]); err != nil {
				return
			}
		}		
	}	
	
	return
}



func ConsolidatePatches(baseline string, patches []string) (err error){

	for _, patch := range patches {	
		if err = MergePatch(baseline, patch); err != nil {
			slog.Error(err)
			return
		} 
	}	
	return
}


func CreateRangeFile(filepath string, patches []Patch) (err error) {

	var fileStat syscall.Stat_t
	if err = syscall.Stat(filepath, &fileStat); err != nil {
		return
	}	
	
	rangeFile := S3_TRITON_ROOT + filepath + ".range"
	
	var outfile *os.File
	
	if outfile, err = os.OpenFile(rangeFile, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0666); err != nil {
		slog.Error(err)
		return
	}
	defer outfile.Close()	
	
	buf := "bytes "
	if len(patches) == 1 && patches[0].Type == PATCH_TRUNCATE {
		buf = buf + "*/" + strconv.FormatInt(fileStat.Size, 10)
		
		outfile.Write([]byte(buf))
		return
	}
	
	for i, patch := range patches {
		if i == 0 {
			buf = buf + strconv.FormatInt(patch.Offset, 10) + "-" + 
			          strconv.FormatInt(patch.Offset + patch.Size - 1, 10) + "/" +
			          strconv.FormatInt(fileStat.Size, 10)
		} else {
			if patch.Size > 0 {
				buf = buf + ", " + strconv.FormatInt(patch.Offset, 10) + "-" +
						  strconv.FormatInt(patch.Offset + patch.Size - 1, 10) + "/*"
			}
		}
		
		if patch.Type == PATCH_TRUNCATE && i != len(patches) - 1 {
			err = errors.New("patch truncate format error")
			return
		}
		
	}
	
	outfile.Write([]byte(buf))
	return
}


func CreatePatch(filepath string) (err error) {
	
	var fileStat syscall.Stat_t
	if err = syscall.Stat(filepath, &fileStat); err != nil {
		slog.Error(err)
		return
	}
	
	isBaseline := IsBaseline(filepath)	
	
	var rdiffBlocks []RDiffBlock
	if rdiffBlocks, err = MakeRDiffBlocks(filepath); err != nil && err != io.EOF {
		slog.Error(err)
		return
	}
	
	if isBaseline {
		if err = UpdateFileMetaData(filepath, rdiffBlocks, SHAValue{}, true); err != nil {
			slog.Error(err)
		}
		return
	}
	
	var metadata FileMetaData
	if metadata, err = GetFileMetaData(filepath); err != nil {
		slog.Error(err)
		return
	}

	curRdiffLen := len(rdiffBlocks)
	orgRdiffLen := len(metadata.PatchState)
	
	fileSize := rdiffBlocks[curRdiffLen - 1].Offset + rdiffBlocks[curRdiffLen - 1].Size
	minLen := Minimum(curRdiffLen, orgRdiffLen).(int)
	
	var patches []Patch
	
	for i := 0; i < minLen; i++ {
		if 	rdiffBlocks[i].Signature != metadata.PatchState[i].Signature {
			patches = append(patches, Patch{rdiffBlocks[i].Offset, rdiffBlocks[i].Size, PATCH_CHANGE})
		}
	}
	
	if curRdiffLen == orgRdiffLen && len(patches) == 0 {		
		if _, err := os.Stat(S3_TRITON_ROOT + filepath + ".patch"); os.IsNotExist(err) {
			isBaseline = true
		} else if err == nil {
			isBaseline = false
		}			
		
		if err = UpdateFileMetaData(filepath, rdiffBlocks, metadata.PatchHash, isBaseline); err != nil {
			slog.Error(err)
		}		
		return
	}
	
	if curRdiffLen > minLen {
		for i := minLen; i < curRdiffLen; i++ {
			patches = append(patches, Patch{rdiffBlocks[i].Offset, rdiffBlocks[i].Size, PATCH_CHANGE})
		}
	} else if orgRdiffLen >= minLen {
		patches = append(patches, Patch{fileSize, 0, PATCH_TRUNCATE})
	}

	patchFile := S3_TRITON_ROOT + filepath + ".patch"
	
	var infile *os.File
	var outfile *os.File
	
	if outfile, err = os.OpenFile(patchFile, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0666); err != nil {
		slog.Error(err)
		return
	}
	defer outfile.Close()
	outfile.WriteString(strconv.Itoa(len(patches)) + "\n")
	
	for _, patch := range patches {
		outfile.WriteString(strconv.FormatInt(patch.Offset, 10) + ":" + strconv.FormatInt(patch.Size, 10) + ":" +
			                strconv.FormatInt(int64(patch.Type), 10) + "\n")
	}
	
	if infile, err = os.Open(filepath); err != nil {
		slog.Error(err)
		return
	}
	defer infile.Close()
	
	buf := make([]byte, RDIFF_BLOCKSIZE)
	
	for _, patch := range patches {
		if patch.Size == 0 {
			break
		}
		
		var rc int
		rc, err = infile.ReadAt(buf, patch.Offset)
		if err != nil && err != io.EOF {
			slog.Error(err)
			return
		}
		
		if rc < RDIFF_BLOCKSIZE {
			buf = buf[:rc]
		}
			
		outfile.Write(buf)
	}
	
	if err = outfile.Sync(); err != nil {
		slog.Error(err)
		return		
	}
	
	if err = CreateRangeFile(filepath, patches); err != nil {
		slog.Error(err)
		return
	}
	
	if err = UpdateFileMetaData(filepath, rdiffBlocks, metadata.PatchHash, false); err != nil {
		slog.Error(err)
	}
	
	return
}
