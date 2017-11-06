package s3manager_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func cpLoggingSvc(ignoreOps []string) (*s3.S3, *[]string, *[]interface{}) {
	var (
		m sync.Mutex

		names  = []string{}
		params = []interface{}{}
	)
	partNum := 0
	svc := s3.New(unit.Session)
	svc.Handlers.Unmarshal.Clear()
	svc.Handlers.UnmarshalMeta.Clear()
	svc.Handlers.UnmarshalError.Clear()
	svc.Handlers.Send.Clear()
	svc.Handlers.Send.PushBack(func(r *request.Request) {
		m.Lock()
		defer m.Unlock()

		if !contains(ignoreOps, r.Operation.Name) {
			names = append(names, r.Operation.Name)
			params = append(params, r.Params)
		}

		r.HTTPResponse = &http.Response{
			StatusCode: 200,
			Body:       ioutil.NopCloser(bytes.NewReader([]byte{})),
		}

		switch data := r.Data.(type) {
		case *s3.CopyPartResult:
			partNum++
			data.ETag = aws.String(fmt.Sprintf("ETAG%d", partNum))
		case *s3.CopyObjectOutput:
			data.VersionId = aws.String("VERSION-ID")
		case *s3.UploadPartCopyOutput:

		}
	})
	return svc, &names, &params
}

func TestCopyOrder(t *testing.T) {
	s, ops, args := cpLoggingSvc(emptyList)

	c := s3manager.NewCopierWithClient(s)

	err := c.Copy(s3manager.CopyInput{
		CopyObjectInput: &s3manager.CopyObjectInput{
			Bucket:     aws.String("foo"),
			Key:        aws.String("key"),
			CopySource: aws.String("other.bucket/bar"),
		},
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	expected := []string{"CopyObject"}
	if !reflect.DeepEqual(expected, *ops) {
		t.Errorf("wanted %v, got %v", expected, *ops)
	}

	if args != nil {
		t.Errorf("wanted %v, got %v", []interface{}{}, *args)
	}
}
