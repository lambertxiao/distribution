package us3

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"

	ufsdk "github.com/ufilesdk-dev/ufile-gosdk"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
)

const driverName = "us3"

type DriverParameters struct {
	PublicKey       string // *************
	PrivateKey      string // *************
	Api             string // api.ucloud.cn
	Bucket          string // test-hzy
	Regin           string // cn-sh2
	Endpoint        string // cn-sh2.ufileos.com
	VerifyUploadMD5 bool   // false
	Rootdirectory   string // ~/my_images
}

// ============================= init ===================================

func init() {
	factory.Register(driverName, &us3DriverFactory{})
}

// ============================ factory =================================

type us3DriverFactory struct{}

// Implement factory.StorageDriverFactory interface
func (factory *us3DriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// ============================= dirver =================================

type driver struct {
	// TODO(zengyan) driver struct
	Req *ufsdk.UFileRequest
}

type baseEmbed struct {
	base.Base
}

type Driver struct {
	baseEmbed
}

func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	param := DriverParameters{}
	return New(param)
}

func New(params DriverParameters) (*Driver, error) {
	// TODO(zengyan) 哪些是 option 的，需要对是否为 nil "" bool 进行判断
	config := &ufsdk.Config{
		PublicKey:       params.PublicKey,
		PrivateKey:      params.PrivateKey,
		BucketHost:      params.Api,
		BucketName:      params.Bucket,
		FileHost:        params.Endpoint,
		VerifyUploadMD5: params.VerifyUploadMD5,
		Endpoint:        params.Endpoint,
	}

	req, err := ufsdk.NewFileRequest(config, nil)
	if err != nil {
		return nil, err
	}

	d := &driver{
		Req: req,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface
func (d *driver) Name() string {
	return driverName
}

func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	// 默认采取流式下载
	// TODO(zengyan) 以下的方法是否可优化？拷贝次数太多？
	arr := bytes.NewBuffer(nil)
	writer := bufio.NewWriter(arr)
	err := d.Req.DownloadFile(writer, path)
	if err != nil {
		return nil, err
	}
	writer.Flush()
	return arr.Bytes(), err

	// TODO(zengyan) get 为什么没有 NoSuchKey 的逻辑？？？
}

func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	// TODO(zengyan) 这里需要把 RootDirectory 与 path 拼接起来吗

	if len(contents) >= 4*1024*1024 { // contents >= 4M 采用分片流式上传
		return d.Req.IOMutipartAsyncUpload(bytes.NewReader(contents), path, d.getContentType())
	} else { // contents < 4M 采用普通流式上传
		return d.Req.IOPut(bytes.NewReader(contents), path, d.getContentType())
	}

	// TODO(zengyan) put 为什么没有 NoSuchKey 的逻辑？？？
}

func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {}

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
}

func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {}

func (d *driver) List(ctx context.Context, opath string) ([]string, error) {}

func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {}

func (d *driver) Delete(ctx context.Context, path string) error {}

func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
}

func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

// ============================= writer ==================================

type writer struct {
	// TODO(zengyan) writer struct
}

// Implement the storagedriver.FileWriter interface
func (w *writer) Write(p []byte) (int, error) {}

func (w *writer) Close() error {}

func (w *writer) Size() int64 {}

func (w *writer) Cancel() error {}

func (w *writer) Commit() error {}
