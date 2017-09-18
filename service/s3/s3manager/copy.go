package s3manager

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// DefaultCopyPartSize declares the default size of chunks to get copied. It is
// currently set dumbly to 500MB. So that the maximum object size (5TB) will
// work without exceeding the maximum part count (10,000).
const DefaultCopyPartSize = 1024 * 1024 * 500

// DefaultCopyConcurrency sets the number of parts to request copying at once.
const DefaultCopyConcurrency = 64

// TODO(ro) 2017-09-16 delete?
// DefaultCopyTimeout is the max time we expect the copy operation to take. For
// a lambda < 5 minutes is best, but for a large copy it could take hours. 5TB
// max file size at 1Gbps ~= 12.5 hours. So with leeway...
const DefaultCopyTimeout = 18 * time.Hour

// CopyInput contains all the input necessary for copy requests to Amazon S3.
// Please also see https://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/CopyObjectRequest
type Copy2Input struct {
	// The canned ACL to apply to the object.
	ACL *string `location:"header" locationName:"x-amz-acl" type:"string" enum:"ObjectCannedACL"`

	// Bucket is a required field
	Bucket *string `location:"uri" locationName:"Bucket" type:"string" required:"true"`

	// Specifies caching behavior along the request/reply chain.
	CacheControl *string `location:"header" locationName:"Cache-Control" type:"string"`

	// Specifies presentational information for the object.
	ContentDisposition *string `location:"header" locationName:"Content-Disposition" type:"string"`

	// Specifies what content encodings have been applied to the object and thus
	// what decoding mechanisms must be applied to obtain the media-type referenced
	// by the Content-Type header field.
	ContentEncoding *string `location:"header" locationName:"Content-Encoding" type:"string"`

	// The language the content is in.
	ContentLanguage *string `location:"header" locationName:"Content-Language" type:"string"`

	// A standard MIME type describing the format of the object data.
	ContentType *string `location:"header" locationName:"Content-Type" type:"string"`

	// The name of the source bucket and key name of the source object, separated
	// by a slash (/). Must be URL-encoded.
	//
	// CopySource is a required field
	CopySource *string `location:"header" locationName:"x-amz-copy-source" type:"string" required:"true"`

	// Copies the object if its entity tag (ETag) matches the specified tag.
	CopySourceIfMatch *string `location:"header" locationName:"x-amz-copy-source-if-match" type:"string"`

	// Copies the object if it has been modified since the specified time.
	CopySourceIfModifiedSince *time.Time `location:"header" locationName:"x-amz-copy-source-if-modified-since" type:"timestamp" timestampFormat:"rfc822"`

	// Copies the object if its entity tag (ETag) is different than the specified
	// ETag.
	CopySourceIfNoneMatch *string `location:"header" locationName:"x-amz-copy-source-if-none-match" type:"string"`

	// Copies the object if it hasn't been modified since the specified time.
	CopySourceIfUnmodifiedSince *time.Time `location:"header" locationName:"x-amz-copy-source-if-unmodified-since" type:"timestamp" timestampFormat:"rfc822"`

	// Specifies the algorithm to use when decrypting the source object (e.g., AES256).
	CopySourceSSECustomerAlgorithm *string `location:"header" locationName:"x-amz-copy-source-server-side-encryption-customer-algorithm" type:"string"`

	// Specifies the customer-provided encryption key for Amazon S3 to use to decrypt
	// the source object. The encryption key provided in this header must be one
	// that was used when the source object was created.
	CopySourceSSECustomerKey *string `location:"header" locationName:"x-amz-copy-source-server-side-encryption-customer-key" type:"string"`

	// Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
	// Amazon S3 uses this header for a message integrity check to ensure the encryption
	// key was transmitted without error.
	CopySourceSSECustomerKeyMD5 *string `location:"header" locationName:"x-amz-copy-source-server-side-encryption-customer-key-MD5" type:"string"`

	// The date and time at which the object is no longer cacheable.
	Expires *time.Time `location:"header" locationName:"Expires" type:"timestamp" timestampFormat:"rfc822"`

	// Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
	GrantFullControl *string `location:"header" locationName:"x-amz-grant-full-control" type:"string"`

	// Allows grantee to read the object data and its metadata.
	GrantRead *string `location:"header" locationName:"x-amz-grant-read" type:"string"`

	// Allows grantee to read the object ACL.
	GrantReadACP *string `location:"header" locationName:"x-amz-grant-read-acp" type:"string"`

	// Allows grantee to write the ACL for the applicable object.
	GrantWriteACP *string `location:"header" locationName:"x-amz-grant-write-acp" type:"string"`

	// Key is a required field
	Key *string `location:"uri" locationName:"Key" min:"1" type:"string" required:"true"`

	// A map of metadata to store with the object in S3.
	Metadata map[string]*string `location:"headers" locationName:"x-amz-meta-" type:"map"`

	// Specifies whether the metadata is copied from the source object or replaced
	// with metadata provided in the request.
	MetadataDirective *string `location:"header" locationName:"x-amz-metadata-directive" type:"string" enum:"MetadataDirective"`

	// Confirms that the requester knows that she or he will be charged for the
	// request. Bucket owners need not specify this parameter in their requests.
	// Documentation on downloading objects from requester pays buckets can be found
	// at http://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html
	RequestPayer *string `location:"header" locationName:"x-amz-request-payer" type:"string" enum:"RequestPayer"`

	// Specifies the algorithm to use to when encrypting the object (e.g., AES256).
	SSECustomerAlgorithm *string `location:"header" locationName:"x-amz-server-side-encryption-customer-algorithm" type:"string"`

	// Specifies the customer-provided encryption key for Amazon S3 to use in encrypting
	// data. This value is used to store the object and then it is discarded; Amazon
	// does not store the encryption key. The key must be appropriate for use with
	// the algorithm specified in the x-amz-server-side​-encryption​-customer-algorithm
	// header.
	SSECustomerKey *string `location:"header" locationName:"x-amz-server-side-encryption-customer-key" type:"string"`

	// Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
	// Amazon S3 uses this header for a message integrity check to ensure the encryption
	// key was transmitted without error.
	SSECustomerKeyMD5 *string `location:"header" locationName:"x-amz-server-side-encryption-customer-key-MD5" type:"string"`

	// Specifies the AWS KMS key ID to use for object encryption. All GET and PUT
	// requests for an object protected by AWS KMS will fail if not made via SSL
	// or using SigV4. Documentation on configuring any of the officially supported
	// AWS SDKs and CLI can be found at http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingAWSSDK.html#specify-signature-version
	SSEKMSKeyId *string `location:"header" locationName:"x-amz-server-side-encryption-aws-kms-key-id" type:"string"`

	// The Server-side encryption algorithm used when storing this object in S3
	// (e.g., AES256, aws:kms).
	ServerSideEncryption *string `location:"header" locationName:"x-amz-server-side-encryption" type:"string" enum:"ServerSideEncryption"`

	// The type of storage to use for the object. Defaults to 'STANDARD'.
	StorageClass *string `location:"header" locationName:"x-amz-storage-class" type:"string" enum:"StorageClass"`

	// The tag-set for the object destination object this value must be used in
	// conjunction with the TaggingDirective. The tag-set must be encoded as URL
	// Query parameters
	Tagging *string `location:"header" locationName:"x-amz-tagging" type:"string"`

	// Specifies whether the object tag-set are copied from the source object or
	// replaced with tag-set provided in the request.
	TaggingDirective *string `location:"header" locationName:"x-amz-tagging-directive" type:"string" enum:"TaggingDirective"`

	// If the bucket is configured as a website, redirects requests for this object
	// to another object in the same bucket or to an external URL. Amazon S3 stores
	// the value of this header in the object metadata.
	WebsiteRedirectLocation *string `location:"header" locationName:"x-amz-website-redirect-location" type:"string"`
}

// TODO(ro) 2017-09-16 delete
type object interface {
	Bucket() *string
	Key() *string
	CopySourceString() *string
	String() string
	Size() int
}

// TODO(ro) 2017-09-17 delete?
// CopyInput holds the input paramters for Copier.Copy.
type CopyInput struct {
	Source object
	Dest   object

	Delete    bool
	SrcRegion *string
	Region    *string
}

// Copier holds the configuration details for copying from an s3 object to another s3 location.
type Copier struct {
	// ConfigProvider is the config to create sessions from.
	ConfigProvider client.ConfigProvider

	// The chunk size for parts.
	PartSize int64

	// How long to run before we quit waiting.
	Timeout time.Duration

	// How many parts to copy at once.
	Concurrency int

	// TODO(ro) 2017-09-07 LeavePartsOnError and abort method.
	// LeavePartsOnError bool

	// The s3 client ot use when copying.
	S3 s3iface.S3API

	// SrcS3 is the source if set, it is a second region. Needed to delete or
	// get the content length of the source object if not provided.
	SrcS3 s3iface.S3API

	// RequestOptions to be passed to the individual calls.
	RequestOptions []request.Option
}

// WithCopierRequestOptions appends to the Copier's API requst options.
func WithCopierRequestOptions(opts ...request.Option) func(*Copier) {
	return func(c *Copier) {
		c.RequestOptions = append(c.RequestOptions, opts...)
	}
}

// NewCopier creates a new Copier instance to copy opbjects concurrently from
// one s3 location to another.
//
// Example:
//	// The session the Copier will use.
// 	sess := session.Must(session.NewSession())
//
// 	// Create a Copier with the session and default options.
// 	copier := s3.manager.NewCopier(sess)
//
// 	// Create a Copier with the session and custom options.
// 	copier := s3.manager.NewCopier(sess, func(c *s3manager.Copier) {
//		c.PartSize := 1024 * 1024 * 64  // 64MB parts
// 	})
func NewCopier(cfgp client.ConfigProvider, options ...func(*Copier)) *Copier {

	c := &Copier{
		ConfigProvider: cfgp,
		PartSize:       DefaultCopyPartSize,
		Timeout:        DefaultCopyTimeout,
		S3:             s3.New(cfgp),
		Concurrency:    DefaultCopyConcurrency,
	}
	for _, option := range options {
		option(c)
	}

	return c
}

// NewCopierWithClient returns a Copier using the provided s3API client. Pass
// in additional functional options to customize the copier's behavior.
//
// Example:
// 	// The session the client will use.
// 	sess := session.Must(session.NewSession())
//
// 	// The client to pass to the Copier.
// 	svc := s3.New(sess)
//
// 	// Create a Copier with the client and default options.
// 	copier := s3manager.NewCopierWithClient(svc)
//
// 	// Create a Copier with the client and custom options.
// 	copier := s3manager.NewCopierWithClient(svc, func (c *Copier){
// 		c.PartSize = 1024 * 1024 * 64 // 64MB parts
// 	})
func NewCopierWithClient(svc s3iface.S3API, options ...func(*Copier)) *Copier {
	c := &Copier{
		S3:          svc,
		PartSize:    DefaultCopyPartSize,
		Concurrency: DefaultCopyConcurrency,
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// Copy copies the source object to the target object. It attemps to
// intelligently perform the copy in parallel for large files. The part size
// and concurrency are configurable through the Copier's parameters by passing
// functional options to the NewCopier functions.
//
// Additional functional options can be provided to the Upload Method to
// configure options for an individual upload. These options are set on a copy
// of the Uploader instance. Therefore, modifying options for an individaul
// copy will not impact the underlying Uploader instance configuration.
//
// Use the WithCopierRequestOptions helper function to pass in that will be
// applied to all API operations made with this uploader.
//
// It is safe to call this method concurrently from multiple goroutines.
//
// Example:
// 	// Copy input parameters
// 	p := &s3manager.CopyInput{
// 		}
//
// 	// Copy the file
// 	err := copier.Copy(p)
//
// 	// Copy with different options.
// 	err := copier.Copy(p, func(c *s3manager.Copier) {
// 		c.PartSize = 1024 * 1024 * 10 // 10MB parts.
//		c.Concurrency = 32 // copy 32 parts concurrently
// 		})
func (c Copier) Copy(i CopyInput, options ...func(*Copier)) error {
	return c.CopyWithContext(context.Background(), i, options...)
}

// CopyWithContext performs Copy with the provided context.Context.
//
// CopyWithContext is the same as Copy with additional support for Context
// input parameters. The Context MUST NOT be nil. passing a nil Context will
// panic. Use the context to add deadlines, cancelation, etc...
//
// Additional functional options can be provided to the Upload Method to
// configure options for an individual upload. These options are set on a copy
// of the Uploader instance. Therefore, modifying options for an individaul
// copy will not impact the underlying Uploader instance configuration.
//
// Use the WithCopierRequestOptions helper function to pass in that will be
// applied to all API operations made with this uploader.
//
// It is safe to call this method concurrently from multiple goroutines.
func (c Copier) CopyWithContext(ctx aws.Context, input CopyInput, options ...func(*Copier)) error {
	// TODO(ro) 2017-09-07 should cancel be external?
	ctx, cancel := context.WithCancel(ctx)
	impl := copier{in: input, cfg: c, ctx: ctx, cancel: cancel, wg: &sync.WaitGroup{}}

	// Apply functional options to the copy of the config.
	for _, option := range options {
		option(&impl.cfg)
	}

	impl.cfg.RequestOptions = append(impl.cfg.RequestOptions, request.WithAppendUserAgent("S3Manager"))

	if s, ok := c.S3.(maxRetrier); ok {
		impl.maxRetries = s.MaxRetries()
	}

	return impl.copy()
}

// copier is the internal implementation of Copier.
type copier struct {
	ctx           aws.Context
	cancel        context.CancelFunc
	cfg           Copier
	contentLength int64

	in      CopyInput
	parts   []*s3.CompletedPart
	results chan copyPartResult
	work    chan multipartCopyInput

	wg *sync.WaitGroup
	m  *sync.Mutex

	err error

	maxRetries int
}

func (c copier) getContentLength() error {
	var size int64
	size = int64(c.in.Source.Size())
	// If less than 1 we want to double check, because unset == 0. We can make
	// it a pointer and check for nil later.
	if size <= 0 {
		info, err := c.objectInfo(c.in.Source)
		if err != nil {
			return err
		}
		size = *info.ContentLength
	}
	c.contentLength = size
	return nil
}

// init sets default options if they are 0.
func (c copier) init() error {
	if c.cfg.Concurrency == 0 {
		c.cfg.Concurrency = DefaultCopyConcurrency
	}
	if c.cfg.PartSize == 0 {
		c.cfg.PartSize = DefaultCopyPartSize
	}

	if c.cfg.PartSize < MinUploadPartSize {
		msg := fmt.Sprintf("part size must be at least %d bytes", MinUploadPartSize)
		return awserr.New("ConfigError", msg, nil)
	}

	// Set up a source region. This is to get the source size if it isn't
	// explicitly provided and for deleting the original source if the option
	// is set.
	if c.in.SrcRegion != nil && *c.in.SrcRegion != "" {
		srcSess := session.Must(session.NewSession(
			&aws.Config{Region: c.in.SrcRegion}))
		c.cfg.SrcS2 = s3.New(srcSess)
	} else {
		c.cfg.SrcS2 = c.cfg.S3
	}

	err := c.getContentLength()
	if err != nil {
		msg := fmt.Sprintf("failed to get content length: %s", err.Error())
		return awserr.New("RequestError", msg, nil)
	}
	return nil
}

// copy is the internal implementation of Copy.
func (c copier) copy() error {
	err := c.init()

	// If there's a request to delete the source copy, do it on exit if there
	// was no error copying.
	if c.in.Delete {
		defer func() {
			if c.err != nil {
				return
			}
			c.deleteObject(c.in.Source)
		}()
	}

	// If smaller than part size, just copy.
	if c.contentLength < c.cfg.PartSize {
		return c.copyObject()

	}

	// Otherwise do a multipart copy.
	uid, err := c.startMulipart(c.in.Dest)
	if err != nil {
		return err
	}
	logMessage(c.cfg.S3, aws.LogDebug, fmt.Sprintf(
		"Started MultipartUpload %s\n", *uid))

	partCount := int(math.Ceil(float64(c.contentLength) / float64(c.cfg.PartSize)))
	c.parts = make([]*s3.CompletedPart, partCount)
	c.results = make(chan copyPartResult, c.cfg.Concurrency)
	c.work = make(chan multipartCopyInput, c.cfg.Concurrency)
	var partNum int64
	size := c.contentLength
	go func() {
		for size >= 0 {
			offset := c.cfg.PartSize * partNum
			endByte := offset + c.cfg.PartSize - 1
			if endByte >= c.contentLength {
				endByte = c.contentLength - 1
			}
			mci := multipartCopyInput{
				Part:            partNum + 1,
				Bucket:          c.in.Dest.Bucket(),
				Key:             c.in.Dest.Key(),
				CopySource:      c.in.Source.CopySourceString(),
				CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", offset, endByte)),
				UploadID:        uid,
			}
			c.wg.Add(1)
			c.work <- mci
			partNum++
			size -= c.cfg.PartSize
			if size <= 0 {
				break
			}

		}
		close(c.work)
	}()

	for i := 0; i < c.cfg.Concurrency; i++ {
		go c.copyPart()
	}
	go c.collect()
	c.wait()

	return c.complete(uid)
}

func (c copier) copyObject() error {
	coi := &s3.CopyObjectInput{
		Bucket:     c.in.Dest.Bucket(),
		Key:        c.in.Dest.Key(),
		CopySource: c.in.Source.CopySourceString(),
	}
	_, err := c.cfg.S3.CopyObject(coi)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			logMessage(c.cfg.S3, aws.LogDebug, fmt.Sprintf(
				"Failed to get source info for %s: %s\n", c.in.Source, aerr.Error()))
		} else {
			logMessage(c.cfg.S3, aws.LogDebug, fmt.Sprintf(
				"Failed to get source info for %s: %s\n", c.in.Source, err))
		}
		return err
	}
	return nil

}

// collect adds the completed parts to the parts array at the appropriate
// index.
func (c copier) collect() {
	for r := range c.results {
		c.parts[r.Part-1] = &s3.CompletedPart{
			ETag:       r.CopyPartResult.ETag,
			PartNumber: aws.Int64(r.Part)}
	}
}

// wait prevents the call from completing until work in the goroutines is
// finished, we timeout, or a signal is caught.
func (c copier) wait() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		// Wait until all the results are in, then close the channel.
		c.wg.Wait()
		close(c.results)
		done <- struct{}{}
	}()

	select {
	case <-done:
		return
	case sig := <-sigs:
		c.cancel()
		logMessage(c.cfg.S3, aws.LogDebug, fmt.Sprintf(
			"Caught signal %s\n", sig))
		os.Exit(0)
	case <-time.After(c.cfg.Timeout):
		c.cancel()
		logMessage(c.cfg.S3, aws.LogDebug, fmt.Sprintf(
			"Copy timed out in %s\n", c.cfg.Timeout))
		os.Exit(1)
	}
}

func (c copier) getErr() error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.err
}

func (c copier) setErr(e error) {
	c.m.Lock()
	defer c.m.Unlock()

	c.err = e
}

func (c copier) copyPart() {
	var err error
	var resp *s3.UploadPartCopyOutput
	for in := range c.work {
		upci := &s3.UploadPartCopyInput{
			Bucket:          in.Bucket,
			Key:             in.Key,
			CopySource:      in.CopySource,
			CopySourceRange: in.CopySourceRange,
			PartNumber:      aws.Int64(in.Part),
			UploadId:        in.UploadID,
		}
		for retry := 0; retry <= c.maxRetries; retry++ {
			resp, err = c.cfg.S3.UploadPartCopy(upci)
			if err != nil {
				logMessage(c.cfg.S3, aws.LogDebug, fmt.Sprintf(
					"Error: %s\n Part: %d\n Input %#v\n", err, in.Part, *upci))
				continue
			}
			c.results <- copyPartResult{
				Part:           in.Part,
				CopyPartResult: resp.CopyPartResult}
			break
		}
		if err != nil {
			c.setErr(err)
		}
		c.wg.Done()
	}
	return
}

// complete finishes this multipart copy.
func (c copier) complete(uid *string) error {
	cmui := &s3.CompleteMultipartUploadInput{
		Bucket:   c.in.Dest.Bucket(),
		Key:      c.in.Dest.Key(),
		UploadId: uid,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: c.parts,
		},
	}
	_, err := c.cfg.S3.CompleteMultipartUpload(cmui)
	if err != nil {
		logMessage(c.cfg.S3, aws.LogDebug, fmt.Sprintf(
			"Failed to complete copy for %s: %s\n", *c.in.Source.CopySourceString(), err))
		return err
	}
	return nil
}

type copyPartResult struct {
	Part int64
	*s3.CopyPartResult
}

type multipartCopyInput struct {
	Part int64

	Bucket          *string
	CopySource      *string
	CopySourceRange *string
	Key             *string
	UploadID        *string
}

func (c copier) startMulipart(o object) (*string, error) {
	cmui := &s3.CreateMultipartUploadInput{
		Bucket: c.in.Dest.Bucket(),
		Key:    c.in.Dest.Key(),
	}
	resp, err := c.cfg.S3.CreateMultipartUpload(cmui)
	if err != nil {
		return nil, err
	}
	return resp.UploadId, nil
}

func (c copier) objectInfo(o object) (*s3.HeadObjectOutput, error) {
	info, err := c.cfg.SrcS3.HeadObject(&s3.HeadObjectInput{
		Bucket: c.in.Source.Bucket(),
		Key:    c.in.Source.Key(),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			logMessage(c.cfg.S3, aws.LogDebug, fmt.Sprintf(
				"Failed to get source object info for %s: %s\n", c.in.Source, aerr.Error()))
		} else {
			logMessage(c.cfg.S3, aws.LogDebug, fmt.Sprintf(
				"Failed to get source object info for %s: %s\n", c.in.Source, err))
		}
		return nil, err
	}
	return info, nil
}

// deleteObject deletes and object. We can use it after copy, say for a move.
func (c *copier) deleteObject(o object) {
	params := &s3.DeleteObjectInput{
		Bucket: o.Bucket(),
		Key:    o.Key(),
	}
	_, err := c.cfg.SrcS3.DeleteObject(params)
	if err != nil {
		logMessage(c.cfg.S3, aws.LogDebug, fmt.Sprintf(
			"Failed to delete %s: %s", o, err))
	}
}

func (c *copier) getBucketLocation(bucket string) (string, error) {
	region, err := s3manager.GetBucketRegion(c.ctx, sess, bucket, "us-west-2")
	if err != nil {
		return "", err
	}
	return region, nil
}
