package s3

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/docker/docker-registry/storagedriver"
	"github.com/docker/docker-registry/storagedriver/factory"
)

const driverName = "s3"

// minChunkSize defines the minimum multipart upload chunk size
// S3 API requires multipart upload chunks to be at least 5MB
const chunkSize = 5 * 1024 * 1024

// listMax is the largest amount of objects you can request from S3 in a list call
const listMax = 1000

func init() {
	factory.Register(driverName, &s3DriverFactory{})
}

// s3DriverFactory implements the factory.StorageDriverFactory interface
type s3DriverFactory struct{}

func (factory *s3DriverFactory) Create(parameters map[string]string) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// Driver is a storagedriver.StorageDriver implementation backed by Amazon S3
// Objects are stored at absolute keys in the provided bucket
type Driver struct {
	S3      *s3.S3
	Bucket  *s3.Bucket
	Encrypt bool
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - accesskey
// - secretkey
// - region
// - bucket
// - encrypt
func FromParameters(parameters map[string]string) (*Driver, error) {
	accessKey, ok := parameters["accesskey"]
	if !ok || accessKey == "" {
		return nil, fmt.Errorf("No accesskey parameter provided")
	}

	secretKey, ok := parameters["secretkey"]
	if !ok || secretKey == "" {
		return nil, fmt.Errorf("No secretkey parameter provided")
	}

	regionName, ok := parameters["region"]
	if !ok || regionName == "" {
		return nil, fmt.Errorf("No region parameter provided")
	}
	region := aws.GetRegion(regionName)
	if region.Name == "" {
		return nil, fmt.Errorf("Invalid region provided: %v", region)
	}

	bucket, ok := parameters["bucket"]
	if !ok || bucket == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	encrypt, ok := parameters["encrypt"]
	if !ok {
		return nil, fmt.Errorf("No encrypt parameter provided")
	}

	encryptBool, err := strconv.ParseBool(encrypt)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse the encrypt parameter: %v", err)
	}
	return New(accessKey, secretKey, region, encryptBool, bucket)
}

// New constructs a new Driver with the given AWS credentials, region, encryption flag, and
// bucketName
func New(accessKey string, secretKey string, region aws.Region, encrypt bool, bucketName string) (*Driver, error) {
	auth := aws.Auth{AccessKey: accessKey, SecretKey: secretKey}
	s3obj := s3.New(auth, region)
	bucket := s3obj.Bucket(bucketName)

	if err := bucket.PutBucket(getPermissions()); err != nil {
		s3Err, ok := err.(*s3.Error)
		if !(ok && s3Err.Code == "BucketAlreadyOwnedByYou") {
			return nil, err
		}
	}

	// TODO What if they use this bucket for other things? I can't just clean out the multis
	// TODO Add timestamp checking
	multis, _, err := bucket.ListMulti("", "")
	if err != nil {
		return nil, err
	}

	for _, multi := range multis {
		err := multi.Abort()
		//TODO appropriate to do this error checking?
		if err != nil {
			return nil, err
		}
	}

	return &Driver{s3obj, bucket, encrypt}, nil
}

// Implement the storagedriver.StorageDriver interface

// GetContent retrieves the content stored at "path" as a []byte.
func (d *Driver) GetContent(path string) ([]byte, error) {
	path = strings.TrimLeft(path, "/")
	content, err := d.Bucket.Get(path)
	if err != nil {
		if s3Err, ok := err.(*s3.Error); ok && s3Err.Code == "NoSuchKey" {
			return nil, storagedriver.PathNotFoundError{Path: "/" + path}
		} else {
			return nil, err
		}
	}
	return content, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *Driver) PutContent(path string, contents []byte) error {
	path = strings.TrimLeft(path, "/")
	return d.Bucket.Put(path, contents, d.getContentType(), getPermissions(), d.getOptions())
}

type EmptyReadCloser struct{}

func (rc EmptyReadCloser) Close() error {
	return nil
}

func (rc EmptyReadCloser) Read(p []byte) (n int, err error) {
	return bytes.NewReader([]byte{}).Read(p)
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *Driver) ReadStream(path string, offset int64) (io.ReadCloser, error) {
	path = strings.TrimLeft(path, "/")
	if offset < 0 {
		return nil, storagedriver.InvalidOffsetError{Path: "/" + path, Offset: offset}
	}

	headers := make(http.Header)
	headers.Add("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")

	resp, err := d.Bucket.GetResponseWithHeaders(path, headers)
	if err != nil {
		if s3Err, ok := err.(*s3.Error); ok && s3Err.Code == "NoSuchKey" {
			return nil, storagedriver.PathNotFoundError{Path: "/" + path}
		} else if s3Err, ok := err.(*s3.Error); ok && s3Err.Code == "InvalidRange" {

			return EmptyReadCloser{}, nil
		} else {
			return nil, err
		}
	}
	return resp.Body, nil
}

// WriteStream stores the contents of the provided io.ReadCloser at a location
// designated by the given path.
func (d *Driver) WriteStream(path string, offset int64, reader io.Reader) (totalRead int64, err error) {
	path = strings.TrimLeft(path, "/")
	if offset < 0 {
		return 0, storagedriver.InvalidOffsetError{Path: "/" + path, Offset: offset}
	}

	partNumber := 1
	parts := []s3.Part{}
	var part s3.Part

	multi, err := d.Bucket.InitMulti(path, d.getContentType(), getPermissions(), d.getOptions())
	if err != nil {
		return 0, err
	}

	buf := make([]byte, chunkSize)

	// We never want to leave a dangling multipart upload, our only consistent state is
	// when there is a whole object at path. This is in order to remain consistent with
	// the stat call.
	//
	// Note that if the machine dies before executing the defer, we will be left with a dangling
	// multipart upload, which will eventually be cleaned up, but we will lose all of the progress
	// made prior to the machine crashing.
	defer func() {
		if len(parts) > 0 {
			err = multi.Complete(parts)
			if err != nil {
				multi.Abort()
			}
		}
	}()

	if offset > 0 {
		resp, err := d.Bucket.Head(path, nil)
		if err != nil {
			return 0, err
		}
		if resp.ContentLength < offset {
			return 0, storagedriver.InvalidOffsetError{Path: "/" + path, Offset: offset}
		}

		if resp.ContentLength < chunkSize {
			// If everything written so far is less than the minimum part size of 5MB, we need
			// to fill out the first part up to that minimum.
			current, err := d.ReadStream(path, 0)
			if err != nil {
				return 0, err
			}

			bytesRead, err := io.ReadFull(current, buf[0:offset])
			if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
				return 0, err
			} else if int64(bytesRead) != offset {
				//TODO Maybe a different error? I don't even think this case is reachable...
				return 0, storagedriver.InvalidOffsetError{Path: "/" + path, Offset: offset}
			}

			bytesRead, err = io.ReadFull(reader, buf[offset:])
			totalRead += int64(bytesRead)

			if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
				return totalRead, err
			} else {
				part, err = multi.PutPart(int(partNumber), bytes.NewReader(buf[0:int64(bytesRead)+offset]))
				if err != nil {
					return totalRead, err
				}

			}

		} else {
			// If the file that we already have is larger than 5MB, then we make it the first part
			// of the new multipart upload.
			_, part, err = multi.PutPartCopy(partNumber, s3.CopyOptions{}, d.Bucket.Name+"/"+path)
			if err != nil {
				return 0, err
			}
		}

		parts = append(parts, part)
		partNumber++

		if totalRead+offset < chunkSize {
			return totalRead, nil
		}
	}

	for {
		bytesRead, err := io.ReadFull(reader, buf)
		totalRead += int64(bytesRead)

		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			return totalRead, err
		} else {
			part, err := multi.PutPart(int(partNumber), bytes.NewReader(buf[0:bytesRead]))
			if err != nil {
				return totalRead, err
			}

			parts = append(parts, part)
			partNumber++

			if int64(bytesRead) < chunkSize {
				break
			}
		}
	}

	return totalRead, nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *Driver) Stat(path string) (storagedriver.FileInfo, error) {
	path = strings.TrimLeft(path, "/")
	listResponse, err := d.Bucket.List(path, "", "", 1)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(listResponse.Contents) == 1 {
		if listResponse.Contents[0].Key != path {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = listResponse.Contents[0].Size

			timestamp, err := time.Parse(time.RFC3339Nano, listResponse.Contents[0].LastModified)
			if err != nil {
				return nil, err
			}
			fi.ModTime = timestamp
		}
	} else if len(listResponse.CommonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: "/" + path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *Driver) List(path string) ([]string, error) {
	path = strings.TrimLeft(path, "/")
	if path != "" && path[len(path)-1] != '/' {
		path = path + "/"
	}
	listResponse, err := d.Bucket.List(path, "/", "", listMax)
	if err != nil {
		return nil, err
	}

	files := []string{}
	directories := []string{}

	for {
		for _, key := range listResponse.Contents {
			files = append(files, key.Key)
		}

		for _, commonPrefix := range listResponse.CommonPrefixes {
			directories = append(directories, commonPrefix[0:len(commonPrefix)-1])
		}

		if listResponse.IsTruncated {
			listResponse, err = d.Bucket.List(path, "/", listResponse.NextMarker, listMax)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}

	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *Driver) Move(sourcePath string, destPath string) error {
	sourcePath = strings.TrimLeft(sourcePath, "/")
	destPath = strings.TrimLeft(destPath, "/")

	/* This is terrible, but aws doesn't have an actual move. */
	_, err := d.Bucket.PutCopy(destPath, getPermissions(),
		s3.CopyOptions{Options: d.getOptions(), MetadataDirective: "", ContentType: d.getContentType()},
		d.Bucket.Name+"/"+sourcePath)
	if err != nil {
		return storagedriver.PathNotFoundError{Path: "/" + sourcePath}
	}

	return d.Delete(sourcePath)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *Driver) Delete(path string) error {
	path = strings.TrimLeft(path, "/")
	listResponse, err := d.Bucket.List(path, "", "", listMax)
	if err != nil || len(listResponse.Contents) == 0 {
		return storagedriver.PathNotFoundError{Path: "/" + path}
	}

	s3Objects := make([]s3.Object, listMax)

	for len(listResponse.Contents) > 0 {
		for index, key := range listResponse.Contents {
			s3Objects[index].Key = key.Key
		}

		err := d.Bucket.DelMulti(s3.Delete{Quiet: false, Objects: s3Objects[0:len(listResponse.Contents)]})
		if err != nil {
			return nil
		}

		listResponse, err = d.Bucket.List(path, "", "", listMax)
		if err != nil {
			return err
		}
	}

	return nil
}

// func (d *Driver) getHighestIDMulti(path string) (multi *s3.Multi, err error) {
// 	multis, _, err := d.Bucket.ListMulti(path, "")
// 	if err != nil && !hasCode(err, "NoSuchUpload") {
// 		return nil, err
// 	}

// 	uploadID := ""

// 	if len(multis) > 0 {
// 		for _, m := range multis {
// 			if m.Key == path && m.UploadId >= uploadID {
// 				uploadID = m.UploadId
// 				multi = m
// 			}
// 		}
// 		return multi, nil
// 	}
// 	multi, err = d.Bucket.InitMulti(path, d.getContentType(), getPermissions(), d.getOptions())
// 	return multi, err
// }

// func (d *Driver) getAllParts(path string) (*s3.Multi, []s3.Part, error) {
// 	multi, err := d.getHighestIDMulti(path)
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	parts, err := multi.ListParts()
// 	return multi, parts, err
// }

func hasCode(err error, code string) bool {
	s3err, ok := err.(*aws.Error)
	return ok && s3err.Code == code
}

func (d *Driver) getOptions() s3.Options {
	return s3.Options{SSE: d.Encrypt}
}

func getPermissions() s3.ACL {
	return s3.Private
}

func (d *Driver) getContentType() string {
	return "application/octet-stream"
}
