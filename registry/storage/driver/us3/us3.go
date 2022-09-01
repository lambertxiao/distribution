package us3

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	Rootdirectory   string // /my_images 或 /
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
	Req             *ufsdk.UFileRequest
	PublicKey       string
	PrivateKey      string
	Api             string
	Bucket          string
	Endpoint        string
	VerifyUploadMD5 bool
	Rootdirectory   string
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
		Req:           req,
		Rootdirectory: params.Rootdirectory,
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
	buf := bytes.NewBuffer(nil)
	writer := bufio.NewWriter(buf)
	err := d.Req.DownloadFile(writer, path)
	if err != nil {
		return nil, err
	}
	writer.Flush()
	return buf.Bytes(), err

	// TODO(zengyan) get 为什么没有 NoSuchKey 的逻辑？？？
}

func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	// 猜测：image 的文件名
	// 如：若要 put 的 image 为 106.75.215.32:8080/library/hello-world，则 path 为 /library/hello-world
	// 调用 us3Path 后为 d.RootDirectory+path。如：~/my_images/library/hello-world

	// TODO(zengyan) 这里需要把 RootDirectory 与 path 拼接起来吗
	if len(contents) >= 4*1024*1024 { // contents >= 4M 采用分片流式上传
		return d.Req.IOMutipartAsyncUpload(bytes.NewReader(contents), d.us3Path(path), d.getContentType())
	} else { // contents < 4M 采用普通流式上传
		return d.Req.IOPut(bytes.NewReader(contents), d.us3Path(path), d.getContentType())
	}

	// TODO(zengyan) put 为什么没有 NoSuchKey 的逻辑？？？
}

func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	header := make(http.Header)
	// TODO(zengyan) 不确定 header 格式是否正确
	header.Add("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")

	var err error
	d.Req, err = ufsdk.NewFileRequestWithHeader(&ufsdk.Config{
		PublicKey:       d.PublicKey,
		PrivateKey:      d.PrivateKey,
		BucketHost:      d.Api,
		BucketName:      d.Bucket,
		FileHost:        d.Endpoint,
		VerifyUploadMD5: d.VerifyUploadMD5,
		Endpoint:        d.Endpoint,
	}, header, nil)

	if err != nil {
		return nil, err
	}

	// TODO(zengyan) reader writer readcloser []byte 之间的转换
	err = d.Req.DownloadFile(writer, d.us3Path(path))
	if err != nil {
		return nil, err
	}

	// TODO(zengyan) put 为什么没有 NoSuchKey 的逻辑？？？
}

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
}

func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	// TODO(zengyan) 确定一下 delim 是否为 ""
	list, err := d.Req.ListObjects(d.us3Path(path), "", "", 1)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(list.Contents) == 1 {
		// TODO(zengyan) 由于 ListObjects 是前缀匹配，存在一种情况，path 为真实存在的文件名，但是用户输入 path 时末尾漏了一位。这将导致程序返回 path is a dir
		if list.Contents[0].Key != d.us3Path(path) {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			size, err := strconv.ParseInt(list.Contents[0].Size, 10, 64)
			if err != nil {
				return nil, err
			}
			fi.Size = size
			// TODO(zengyan) 不确定这里返回的 int 类型的 LastModified 是否能按 RFC3339Nano 求出 time.time 类型的值
			timestamp, err := time.Parse(time.RFC3339Nano, strconv.Itoa(list.Contents[0].LastModified))
			if err != nil {
				return nil, err
			}
			fi.ModTime = timestamp
		}
	} else if len(list.CommonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

func (d *driver) List(ctx context.Context, opath string) ([]string, error) {}

func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	err := d.Req.Copy(d.us3Path(destPath), d.Bucket, d.us3Path(sourcePath))
	if err != nil {
		return err
	}
	return d.Delete(ctx, sourcePath)
}

func (d *driver) Delete(ctx context.Context, path string) error {
	// TODO(zengyan) 需要确定到底要删什么东西？
	return d.Req.DeleteFile(d.us3Path(path))
}

func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	methodString := "GET"
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != "GET") {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	expiresIn := 20 * time.Minute // 默认该 URL 有 20 分钟的有效时间
	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresIn = time.Until(et)
		}
	}

	return d.Req.GetPrivateURL(d.us3Path(path), expiresIn), nil
}

func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

func (d *driver) us3Path(path string) string {
	// 将 d.RootDirectory 与 path 拼起来返回
	return strings.TrimLeft(strings.TrimRight(d.Rootdirectory, "/")+path, "/")
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
