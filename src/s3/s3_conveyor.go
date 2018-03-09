package s3

import (
	"os"
	"time"
	"strings"
	"encoding/hex"
	"encoding/base64"
	"github.com/pkg/errors"
	
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	
	"../slog"
	"../bindiff"
)

func init() {
	err := slog.SetSyslog("s3_conveyor")
	if err != nil {
		slog.Error(err)
	}
}

type S3Conveyor struct {
	Sess *session.Session
	Client *s3.S3
	Uploader *s3manager.Uploader
	Downloader *s3manager.Downloader
}


func NewS3Conveyor(region string) *S3Conveyor {
	
	sess := session.New(&aws.Config{Region: aws.String(region)})
		
	return &S3Conveyor{ 
						Sess: sess,
						Client: s3.New(sess),
						Uploader: s3manager.NewUploader(sess), 
						Downloader: s3manager.NewDownloader(sess),
					  }	
}


func (conveyor *S3Conveyor) UploadObject(bucket string, foldInBucket string, filepath string) (err error) {

	if conveyor == nil || conveyor.Uploader == nil {
		slog.Error("No uploader instance")
		
		err = errors.New("No uploader instance")
		return
	}
		
	var metadata bindiff.FileMetaData
	
	if metadata, err = bindiff.GetFileMetaData(filepath); err != nil {
		slog.Errorf("Fail to get meta data for %s: %s", filepath, err.Error())
		return
	}
	
	var uploadfilepath string
	
	if metadata.PatchType == bindiff.FORMAT_BASELINE {
		uploadfilepath = filepath
	} else {
		uploadfilepath = bindiff.S3_TRITON_ROOT + filepath + ".patch"
	}
	
	var file *os.File
	
	if file, err = os.Open(uploadfilepath); err != nil {
		slog.Errorf("Unable to open file %s, %s", uploadfilepath, err)
		return
	}

	defer file.Close()
	
	patchHash := hex.EncodeToString(metadata.PatchHash[:])
	
	// Upload the file's body to S3 bucket as an object with the key being the
	// same as the filename.
	result, err := conveyor.Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),

		// Can also use the `filepath` standard library package to modify the
		// filename as need for an S3 object key. Such as turning abolute path
		// to a relative path.
		Key: aws.String(foldInBucket + "/" + patchHash[:2] + "/" + patchHash[2:4] + "/" + patchHash + ".dat"),

		// The file to be uploaded. io.ReadSeeker is prefered as the Uploader
		// will be able to optimize memory when uploading large content. io.Reader
		// is supported, but will require buffering of the reader's bytes for
		// each part.
		Body: file,
	})

	if err != nil {
		// Print the error and exit.
		slog.Errorf("Unable to upload %s to %s, %v", filepath, bucket, err)
		return
	}

	slog.Infof("location is %s\n", result.Location)
	
	if result.VersionID != nil {
		slog.Infof("VersionID is %s\n", *result.VersionID)
	}
	
	slog.Infof("uploadID is %s\n", result.UploadID)
	slog.Infof("Successfully uploaded %s to %s\n", filepath, bucket)
	
	return
}


func (conveyor *S3Conveyor) DownloadObject(bucket string, filenameInBucket string, downloadfile string) (err error) {

	if conveyor == nil || conveyor.Downloader == nil {
		slog.Error("No downloader instance")
		
		err = errors.New("No downloader instance")
		return
	}

	var file *os.File
	if file, err = os.Create(downloadfile); err != nil {
	    slog.Errorf("failed to create file %s, %v", downloadfile, err)
	    return
	}
	
	defer file.Close()
	
	rc, err := conveyor.Downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key: aws.String(filenameInBucket),
	})

	if err != nil {
		slog.Errorf("Unable to download %s from %s to %s, %v", filenameInBucket, bucket, downloadfile, err)
		return
	}

	slog.Infof("download %d for %s from %s", rc, filenameInBucket, bucket)
	return
}


func (conveyor *S3Conveyor) ListBuckets() (buckets []string, err error) {

	if conveyor == nil || conveyor.Client == nil {
		slog.Error("No s3 client created")
		
		err = errors.New("No s3 client created")
		return
	}
	
	var result *s3.ListBucketsOutput
	result, err = conveyor.Client.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		slog.Error("Failed to list buckets", err)
		return
	}

	for _, bucket := range result.Buckets {		
		buckets = append(buckets, *bucket.Name)
	}
	
	return
}

func (conveyor *S3Conveyor) CheckPathInBucket(bucket string, path string) (found bool, err error) {
	
	if conveyor == nil || conveyor.Client == nil {
		slog.Error("No s3 client created")
		
		err = errors.New("No s3 client created")
		return
	}
	
	if strings.HasSuffix(path, "/") == false  {
		path = path + "/"
	}
	
	var listResponse *s3.ListObjectsV2Output 

	listResponse, err = conveyor.Client.ListObjectsV2(&s3.ListObjectsV2Input{
						Bucket: aws.String(bucket),
						Prefix: aws.String(path),
					});
	
	if err != nil {
	    if aerr, ok := err.(awserr.Error); ok {
	        switch aerr.Code() {
		        case s3.ErrCodeNoSuchBucket:
		            slog.Error(s3.ErrCodeNoSuchBucket, aerr.Error())
		        default:
		            slog.Error(aerr)
	        }
	    } else {
	        // Print the error, cast err to awserr.Error to get the Code and
	        // Message from an error.
	        slog.Error(aerr)
	    }  
	    
	    return  
	}
	
	if len(listResponse.Contents) > 0 {
		found = true
		slog.Infof("Found %s in bucket %s", *listResponse.Prefix, *listResponse.Name)
	} else {
		slog.Infof("%s in bucket %s not existed", *listResponse.Prefix, *listResponse.Name)
	}
	return
}

func (conveyor *S3Conveyor) CreatePathInBucket(bucket string, path string) (err error) {
	
	if conveyor == nil || conveyor.Client == nil {
		slog.Error("No s3 client created")
		
		err = errors.New("No s3 client created")
		return
	}
	
	if strings.HasSuffix(path, "/") == false  {
		path = path + "/"
	}
			
	_, err = conveyor.Client.PutObject(&s3.PutObjectInput{
            Bucket: aws.String(bucket),
            Key:    aws.String(path),
		})
	
	return
}

func (conveyor *S3Conveyor) GeneratePresignedURL(bucket string, foldInBucket string, object string) (url string, etag string, err error) {
	
	if conveyor == nil || conveyor.Client == nil {
		slog.Error("No s3 client created")
		
		err = errors.New("No s3 client created")
		return
	}	
	
	dotPos := strings.LastIndex(object, ".")
	if dotPos != -1 {
		object = object[: dotPos]
	}
	
	if len(object) != 40 {
		err = errors.Errorf("object %s length invalid", object)
		return		
	}
	
	for _, ch := range object {
		if ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f' || ch >= 'A' && ch <= 'F' {
			continue
		} else {
			err = errors.Errorf("Invalid object: %s", object)
			return			
		}
	}
	
	object = object + ".dat"

	req, headResp := conveyor.Client.HeadObjectRequest(&s3.HeadObjectInput{
                        	Bucket: aws.String(bucket),
                        	Key:    aws.String(foldInBucket + "/" + object[0:2] + "/" + object[2:4] + "/" + object),
			            })

	if err = req.Send(); err == nil {
		slog.Info(headResp)
	} else {
		slog.Errorf("Head request to %s failed: %s\n", object, err.Error())
		return
	}

	etag = *headResp.ETag
	etag = etag[1 : len(etag) -1]
	
	slog.Infof("etag is %s\n", etag)
	
	base64Etag := base64.StdEncoding.EncodeToString([]byte(etag))
	slog.Infof("md5 is %s\n", base64Etag)
	
	req.HTTPRequest.Header.Set("Content-MD5", base64Etag)

	if url, err = req.Presign(15 * time.Minute); err != nil {
		slog.Errorf("Failed to sign request", err)
		return
	}	
	
	return
}

