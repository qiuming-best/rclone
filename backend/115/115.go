package _115

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/SheltonZhu/115driver/pkg/driver"
	"github.com/patrickmn/go-cache"
	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/backend/115/crypto"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	domain      = "www.115.com"
	userAgent   = "Mozilla/5.0 115Browser/27.0.6.6"
	ossEndpoint = "https://oss-cn-shenzhen.aliyuncs.com"

	uploadSizeLimit = 5 * 1024 * 1024 * 1024
	defaultMinSleep = 500 * time.Millisecond
	maxSleep        = 4 * time.Second
	decayConstant   = 2
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "115",
		Description: "115 drive",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "uid",
			Help:     "UID from cookie.",
			Required: true,
		}, {
			Name:     "cid",
			Help:     "CID from cookie.",
			Required: true,
		}, {
			Name:     "seid",
			Help:     "SEID from cookie.",
			Required: true,
		}, {
			Name:     "kid",
			Help:     "KID from cookie.",
			Required: true,
		}, {
			Name:     "pacer_min_sleep",
			Default:  defaultMinSleep,
			Help:     "Minimum time to sleep between API calls.",
			Advanced: true,
		}, {
			Name:     "max_connections",
			Default:  2,
			Help:     "Maximum number of concurrent API connections. Lower values reduce scan speed but may be more stable.",
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default: (encoder.Display |
				encoder.EncodeLeftSpace |
				encoder.EncodeRightSpace |
				encoder.EncodeBackSlash |
				encoder.EncodeColon |
				encoder.EncodeAsterisk |
				encoder.EncodeQuestion |
				encoder.EncodeDoubleQuote |
				encoder.EncodeLtGt |
				encoder.EncodePipe |
				encoder.EncodePercent |
				encoder.EncodeInvalidUtf8),
		}, {
			Name:     "page_size",
			Default:  100,
			Help:     "Number of files to list per page. If you have many files, you can increase this number to reduce the number of API calls.",
			Advanced: true,
		}, {
			Name:     "mount_point",
			Help:     "Local mount point to wait for before binding.",
			Advanced: true,
		}, {
			Name:     "bind_path",
			Help:     "Target path to bind mount the remote path to.",
			Advanced: true,
		}},
	})
}

// Options defines the configguration of this backend
type Options struct {
	UID            string               `config:"uid"`
	CID            string               `config:"cid"`
	SEID           string               `config:"seid"`
	KID            string               `config:"kid"`
	PacerMinSleep  fs.Duration          `config:"pacer_min_sleep"`
	CacheExpire    fs.Duration          `config:"cache_expire"`
	MaxConnections int                  `config:"max_connections"`
	PageSize       int64                `config:"page_size"`
	MountPoint     string               `config:"mount_point"`
	BindPath       string               `config:"bind_path"`
	WafSleep       fs.Duration          `config:"waf_sleep"`
	Enc            encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote 115 drive
type Fs struct {
	name     string
	root     string
	opt      Options
	ci       *fs.ConfigInfo
	features *fs.Features
	srv      *rest.Client
	pacer    *fs.Pacer
	cache    *cache.Cache
	client   *driver.Pan115Client
}

// Object describes a 115 object
type Object struct {
	fs          *Fs
	remote      string
	hasMetaData bool
	name        string
	size        int64
	sha1sum     string
	pickCode    string
	fileID      int64
	modTime     time.Time
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if resp != nil && (resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode == http.StatusForbidden ||
		resp.StatusCode == 405 ||
		resp.StatusCode >= 500) {
		if resp.StatusCode == 405 && f.opt.WafSleep > 0 {
			fs.Logf(f, "Risk control (405) detected, sleeping for %v before retrying", f.opt.WafSleep)
			time.Sleep(time.Duration(f.opt.WafSleep))
		}
		// Always try to read body for more context if error is nil OR if we want to ensure detail
		if err == nil {
			body, readErr := rest.ReadBody(resp)
			if readErr != nil {
				err = fmt.Errorf("HTTP error %v (%v) - failed to read body: %w", resp.StatusCode, resp.Status, readErr)
			} else {
				err = fmt.Errorf("HTTP error %v (%v) returned body: %q", resp.StatusCode, resp.Status, body)
			}
		}

		return true, err
	}
	if err != nil {
		return fserrors.ShouldRetry(err), err
	}
	return false, err
}

// ------------------------------------------------------------

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name string, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	if root == "" {
		root = "/"
	}

	// Default internal cache expire to 24h if not set
	if opt.CacheExpire == 0 {
		opt.CacheExpire = fs.Duration(24 * time.Hour)
	}

	ci := fs.GetConfig(ctx)
	f := &Fs{
		name:  name,
		root:  root,
		opt:   *opt,
		ci:    ci,
		srv:   rest.NewClient(&http.Client{}),
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(opt.PacerMinSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		cache: cache.New(time.Duration(opt.CacheExpire), time.Duration(opt.CacheExpire)*2),
	}
	f.srv.SetErrorHandler(func(resp *http.Response) error {
		body, err := rest.ReadBody(resp)
		if err != nil {
			return fmt.Errorf("error reading error out of body: %w", err)
		}
		if resp.StatusCode == http.StatusForbidden {
			time.Sleep(time.Second)
		}
		return fmt.Errorf("HTTP error %v (%v) returned body: %q", resp.StatusCode, resp.Status, body)
	})
	f.pacer.SetMaxConnections(opt.MaxConnections)
	f.srv.SetRoot("https://webapi.115.com")
	f.srv.SetHeader("User-Agent", userAgent)
	f.srv.SetCookie(&http.Cookie{
		Name:     "UID",
		Value:    opt.UID,
		Domain:   domain,
		Path:     "/",
		HttpOnly: true,
	}, &http.Cookie{
		Name:     "CID",
		Value:    opt.CID,
		Domain:   domain,
		Path:     "/",
		HttpOnly: true,
	}, &http.Cookie{
		Name:     "SEID",
		Value:    opt.SEID,
		Domain:   domain,
		Path:     "/",
		HttpOnly: true,
	}, &http.Cookie{
		Name:     "KID",
		Value:    opt.KID,
		Domain:   domain,
		Path:     "/",
		HttpOnly: true,
	})
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	// Initialize 115driver client
	cr := &driver.Credential{}
	// Create cookie string
	cookieStr := fmt.Sprintf("UID=%s;CID=%s;SEID=%s;KID=%s", opt.UID, opt.CID, opt.SEID, opt.KID)
	if err := cr.FromCookie(cookieStr); err != nil {
		return nil, fmt.Errorf("failed to parse cookie: %w", err)
	}
	f.client = driver.New(driver.UA(driver.UA115Browser)).ImportCredential(cr)

	// Explicitly GetUploadInfo to ensure Userkey is correct (cookie KID != Userkey)
	// and UploadMetaInfo is populated.
	if err := f.client.GetUploadInfo(); err != nil {
		return nil, fmt.Errorf("failed to get upload info: %w", err)
	}

	if err := f.client.CookieCheck(); err != nil {
		// Log warning but don't fail, maybe token expired or network issue?
	}

	info, err := f.readMetaDataForPath(ctx, f.remotePath("/"))

	if err == nil && !info.IsDir() {
		f.root = path.Dir(f.root)
		return f, fs.ErrorIsFile
	}

	// Auto bind mount if configured
	if opt.MountPoint != "" && opt.BindPath != "" {
		go func() {
			fs.Logf(f, "Waiting for mount point %s to be ready...", opt.MountPoint)
			// Wait for up to 60 seconds
			for i := 0; i < 60; i++ {
				// Check if mountpoint exists and is a directory
				if fi, err := os.Stat(opt.MountPoint); err == nil && fi.IsDir() {
					// Check if it's actually accessible (rclone mounted) by listing it?
					// Or just try to mount --bind. If it fails, we retry.
					// We interpret MountPoint as the SOURCE path

					fs.Logf(f, "Mount source %s found, attempting bind mount to %s", opt.MountPoint, opt.BindPath)

					// Ensure target is clean before binding
					_ = exec.Command("umount", "-l", opt.BindPath).Run()

					cmd := exec.Command("mount", "--bind", opt.MountPoint, opt.BindPath)
					if out, err := cmd.CombinedOutput(); err == nil {
						fs.Logf(f, "Bind mount successful: %s -> %s", opt.MountPoint, opt.BindPath)
						return
					} else {
						fs.Logf(f, "Bind mount failed (retrying): %v, output: %s", err, string(out))
					}
				}
				time.Sleep(1 * time.Second)
			}
			fs.Errorf(f, "Bind mount timed out after 60s")
		}()
	}

	return f, nil
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("115 %s", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns the supported hash types of the filesystem
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.SHA1)
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
//
// If remote points to a directory then it should return
// ErrorIsDir if possible without doing any extra work,
// otherwise ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	infos, err := f.readDir(ctx, f.remotePath(dir))
	if err != nil {
		return nil, err
	}

	files := make([]fs.DirEntry, 0, len(infos))
	for _, info := range infos {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(info.GetName()))
		file, err := f.itemToDirEntry(ctx, remote, info)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}

	return files, nil
}

// Put in to the remote path with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Put should either
// return an error or upload it properly (rather than e.g. calling panic).
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	f.createObject(src.Remote(), src.ModTime(ctx), src.Size())
	if src.Size() == 0 {
		fs.Debugf(src, "Skipping upload of empty file (not supported by 115)")
		o := f.createObject(src.Remote(), src.ModTime(ctx), src.Size())
		return o, nil
	}
	// TODO: enable file upload

	o := f.createObject(src.Remote(), src.ModTime(ctx), src.Size())
	return o, o.Update(ctx, in, src, options...)
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	paths := strings.Split(f.remotePath(dir), "/")
	prefix := ""
	if len(paths) == 0 {
		return nil
	}

	for idx := 0; idx+1 < len(paths); idx++ {
		prefix += "/" + paths[idx]
		cid, err := f.getDirID(ctx, prefix)
		if err != nil {
			return err
		}
		if cid == -1 {
			fs.Logf(f, "make dir fail, cid is -1")
			return nil
		}

		err = f.makeDir(ctx, cid, paths[idx+1])
		if errors.Is(err, fs.ErrorDirExists) {
			continue
		}
		if err != nil {
			return err
		}

		f.flushDir(prefix)
	}
	return nil
}

// Move src to this remote using server-side move operations.
//
// # This is stored with the remote path given
//
// # It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	if src.Fs().Name() != f.Name() {
		return nil, fs.ErrorCantMove
	}

	srcObj, ok := src.(*Object)
	if !ok {
		fs.Errorf(f, "can not move, not same remote type")
		return nil, fs.ErrorCantMove
	}

	srcParent, srcName := path.Split(f.remotePath(srcObj.remote))
	dstParent, dstName := path.Split(f.remotePath(remote))
	if srcParent == dstParent {
		if srcName == dstName {
			return srcObj, nil
		}

		err := f.renameFile(ctx, srcObj.fileID, dstName)
		if err != nil {
			return nil, err
		}
	} else {
		if srcObj.name != dstName {
			return nil, fs.ErrorCantMove
		}

		cid, err := f.getDirID(ctx, dstParent)
		if err != nil {
			return nil, err
		}
		err = f.moveFile(ctx, srcObj.fileID, cid)
		if err != nil {
			return nil, err
		}
	}

	f.flushDir(srcParent)
	f.flushDir(dstParent)
	return f.NewObject(ctx, remote)
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	if src.Name() != f.Name() {
		return fs.ErrorCantDirMove
	}

	srcFs := src.(*Fs)
	cid, err := f.getDirID(ctx, srcFs.remotePath(srcRemote))
	if err != nil {
		return err
	}

	srcParent, srcName := path.Split(srcFs.remotePath(srcRemote))
	dstParent, dstName := path.Split(f.remotePath(dstRemote))
	if srcParent == dstParent {
		if srcName == dstName {
			return fs.ErrorDirExists
		}

		err = f.renameFile(ctx, cid, dstName)
		if err != nil {
			return err
		}
	} else {
		pid, err := f.getDirID(ctx, dstParent)
		if errors.Is(err, fs.ErrorDirNotFound) {
			newDir, _ := path.Split(path.Clean(dstRemote))
			err = f.Mkdir(ctx, newDir)
			if err != nil {
				return err
			}
			pid, err = f.getDirID(ctx, dstParent)
		}
		if err != nil {
			return err
		}

		err = f.moveFile(ctx, cid, pid)
		if err != nil {
			return err
		}
		if srcName != dstName {
			err = f.renameFile(ctx, cid, dstName)
			if err != nil {
				return err
			}
		}
	}

	for _, dir := range []string{srcParent, dstParent, srcFs.remotePath(srcRemote), f.remotePath(dstRemote)} {
		f.flushDir(dir)
		srcFs.flushDir(dir)
	}
	return nil
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	files, err := f.readDir(ctx, f.remotePath(dir))
	if err != nil {
		return err
	}

	if len(files) > 0 {
		fs.Logf(f, "rmdir, is not empty, dir: %v", dir)
		return fs.ErrorDirectoryNotEmpty
	}

	return f.Purge(ctx, dir)
}

// Purge all files in the directory specified
//
// Implement this if you have a way of deleting all the files
// quicker than just running Remove() on the result of List()
//
// Return an error if it doesn't exist
func (f *Fs) Purge(ctx context.Context, dir string) error {
	info, err := f.readMetaDataForPath(ctx, f.remotePath(dir))
	if err != nil {
		fs.Errorf(f, "purge fail, err: %v, dir: %v", err, f.remotePath(dir))
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return fs.ErrorDirNotFound
		}
		return err
	}
	if !info.IsDir() {
		return fs.ErrorIsFile
	}
	if info.GetCategoryID() == 0 {
		fs.Logf(f, "is root dir, can not purge")
		return nil
	}

	err = f.deleteFile(ctx, info.GetCategoryID(), info.GetParentID())
	if err != nil {
		return err
	}

	parent, _ := path.Split(f.remotePath(dir))
	f.flushDir(parent)

	return nil
}

// About gets quota information from the Fs
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	info, err := f.indexInfo(ctx)
	if err != nil {
		return nil, err
	}

	usage := &fs.Usage{}
	if totalInfo, ok := info.Data.SpaceInfo["all_total"]; ok {
		usage.Total = fs.NewUsageValue(int64(totalInfo.Size))
	}
	if useInfo, ok := info.Data.SpaceInfo["all_use"]; ok {
		usage.Used = fs.NewUsageValue(int64(useInfo.Size))
	}
	if remainInfo, ok := info.Data.SpaceInfo["all_remain"]; ok {
		usage.Free = fs.NewUsageValue(int64(remainInfo.Size))
	}

	return usage, nil
}

func (f *Fs) itemToDirEntry(ctx context.Context, remote string, object *api.FileInfo) (fs.DirEntry, error) {
	if len(remote) > 0 && remote[0] == '/' {
		remote = remote[1:]
	}
	if object.IsDir() {
		d := fs.NewDir(f.opt.Enc.ToStandardPath(remote), object.GetUpdateTime())
		return d, nil
	}

	o, err := f.newObjectWithInfo(ctx, remote, object)
	if err != nil {
		return nil, err
	}

	return o, nil
}

func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *api.FileInfo) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	var err error
	if info != nil {
		err = o.setMetaData(info)
	} else {
		err = o.readMetaData(ctx)
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

func (f *Fs) readMetaDataForPath(ctx context.Context, remotePath string) (*api.FileInfo, error) {
	if remotePath == "/" {
		return &api.FileInfo{CategoryID: "0"}, nil
	}

	remoteDir, filename := path.Split(remotePath)
	infos, err := f.readDir(ctx, remoteDir)
	if err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}

	for _, info := range infos {
		if f.opt.Enc.ToStandardName(info.GetName()) == filename {
			return info, nil
		}
	}

	return nil, fs.ErrorObjectNotFound
}

func (f *Fs) readDir(ctx context.Context, remoteDir string) ([]*api.FileInfo, error) {
	cacheKey := fmt.Sprintf("files:%s", path.Clean(remoteDir))
	if value, ok := f.cache.Get(cacheKey); ok {
		return value.([]*api.FileInfo), nil
	}

	cid, err := f.getDirID(ctx, remoteDir)
	if err != nil {
		return nil, err
	}

	pageSize := f.opt.PageSize
	if pageSize < 500 {
		pageSize = 500
	}
	offset := int64(0)
	files := make([]*api.FileInfo, 0)
	for {
		resp, err := f.getFiles(ctx, remoteDir, cid, pageSize, offset)
		if err != nil {
			return nil, err
		}

		for idx := range resp.Data {
			files = append(files, &resp.Data[idx])
		}

		offset = resp.Offset + pageSize
		if offset >= resp.Count {
			break
		}

		// Pace pagination requests to avoid triggering 115 risk control
		baseSleep := time.Duration(f.opt.PacerMinSleep)
		if baseSleep == 0 {
			baseSleep = 500 * time.Millisecond
		}
		// Add Jitter: 0 to 50% of baseSleep
		jitter := time.Duration(rand.Int63n(int64(baseSleep)/2 + 1))
		time.Sleep(baseSleep + jitter)
	}
	f.cache.SetDefault(cacheKey, files)

	return files, nil
}

func (f *Fs) makeDir(ctx context.Context, pid int64, name string) error {
	opts := rest.Opts{
		Method:          http.MethodPost,
		Path:            "/files/add",
		MultipartParams: url.Values{},
	}
	opts.MultipartParams.Set("pid", strconv.FormatInt(pid, 10))
	opts.MultipartParams.Set("cname", f.opt.Enc.FromStandardName(name))

	var err error
	var info api.MkdirResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return f.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	if !info.State {
		if info.GetErrno() == 20004 {
			return fs.ErrorDirExists
		}
		fs.Logf(f, "make dir fail, pid: %v, name: %v, err: %v, errno: %v", pid, name, info.Error, info.Errno)
		return nil
	}

	return nil
}

func (f *Fs) getDirID(ctx context.Context, remoteDir string) (int64, error) {
	opts := rest.Opts{
		Method:     http.MethodGet,
		Path:       "/files/getid",
		Parameters: url.Values{},
	}
	opts.Parameters.Set("path", f.opt.Enc.FromStandardPath(remoteDir))

	var err error
	var info api.GetDirIDResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return f.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return -1, err
	}
	if !info.State || (remoteDir != "/" && info.CategoryID.String() == "0") {
		fs.Logf(f, "get dir id fail, not state, error: %v, errno: %v, resp: %+v", info.Error, info.Errno, info)
		return -1, fs.ErrorDirNotFound
	}

	cid, _ := info.CategoryID.Int64()
	return cid, nil
}

func (f *Fs) getFiles(ctx context.Context, dir string, cid int64, pageSize int64, offset int64) (*api.GetFilesResponse, error) {
	opts := rest.Opts{
		Method:     http.MethodGet,
		Path:       "/files",
		Parameters: url.Values{},
	}
	opts.Parameters.Set("aid", "1")
	opts.Parameters.Set("cid", strconv.FormatInt(cid, 10))
	opts.Parameters.Set("o", "user_ptime")
	opts.Parameters.Set("asc", "0")
	opts.Parameters.Set("offset", strconv.FormatInt(offset, 10))
	opts.Parameters.Set("show_dir", "1")
	opts.Parameters.Set("limit", strconv.FormatInt(pageSize, 10))
	opts.Parameters.Set("snap", "0")
	opts.Parameters.Set("record_open_time", "1")
	opts.Parameters.Set("format", "json")
	opts.Parameters.Set("fc_mix", "0")

	var err error
	var info api.GetFilesResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return f.shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return nil, err
	}

	return &info, nil
}

func (f *Fs) deleteFile(ctx context.Context, fid int64, pid int64) error {
	fs.Logf(f, "delete file, fid: %v, pid: %v", fid, pid)
	opts := rest.Opts{
		Method:          http.MethodPost,
		Path:            "/rb/delete",
		MultipartParams: url.Values{},
	}
	opts.MultipartParams.Set("fid[0]", strconv.FormatInt(fid, 10))
	opts.MultipartParams.Set("pid", strconv.FormatInt(pid, 10))
	opts.MultipartParams.Set("ignore_warn", "1")

	var err error
	var info api.BaseResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return f.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	if !info.State {
		if errno, ok := info.Errno.(int64); ok && errno == 990009 {
			time.Sleep(time.Second)
		}
		fs.Logf(f, "delete file fail, not state, err: %v, errno: %v", info.Error, info.Errno)
		return nil
	}

	return nil
}

func (f *Fs) moveFile(ctx context.Context, fid int64, pid int64) error {
	fs.Logf(f, "move file, fid: %v, pid: %v", fid, pid)
	opts := rest.Opts{
		Method:          http.MethodPost,
		Path:            "/files/move",
		MultipartParams: url.Values{},
	}
	opts.MultipartParams.Set("fid[0]", strconv.FormatInt(fid, 10))
	opts.MultipartParams.Set("pid", strconv.FormatInt(pid, 10))

	var err error
	var info api.BaseResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return f.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	if !info.State {
		fs.Logf(f, "move file fail, not state")
		return nil
	}

	return nil
}

func (f *Fs) renameFile(ctx context.Context, fid int64, name string) error {
	fs.Logf(f, "rename file, fid: %v, name: %v", fid, name)
	opts := rest.Opts{
		Method:          http.MethodPost,
		Path:            "/files/batch_rename",
		MultipartParams: url.Values{},
	}
	opts.MultipartParams.Set(fmt.Sprintf("files_new_name[%d]", fid), name)

	var err error
	var info api.BaseResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return f.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	if !info.State {
		fs.Logf(f, "rename file fail, not state, err: %v", info.Error)
		return nil
	}

	return nil
}

func (f *Fs) getURL(ctx context.Context, remote string, pickCode string) (string, error) {
	cacheKey := fmt.Sprintf("url:%s", pickCode)
	if value, ok := f.cache.Get(cacheKey); ok {
		return value.(string), nil
	}

	key := crypto.GenerateKey()
	data, _ := json.Marshal(map[string]string{
		"pickcode": pickCode,
	})

	opts := rest.Opts{
		Method:          http.MethodPost,
		RootURL:         "https://proapi.115.com",
		Path:            "/app/chrome/downurl",
		Parameters:      url.Values{},
		MultipartParams: url.Values{},
		ExtraHeaders: map[string]string{
			"User-Agent": "go-resty/2.14.0 (https://github.com/go-resty/resty)",
		},
	}
	opts.Parameters.Add("t", strconv.FormatInt(time.Now().Unix(), 10))
	opts.MultipartParams.Set("data", crypto.Encode([]byte(data), key))
	var err error
	var info api.GetURLResponse
	_, err = f.srv.CallJSON(ctx, &opts, nil, &info)
	if err != nil {
		return "", err
	}

	var encodedData string
	if err := json.Unmarshal(info.Data, &encodedData); err != nil {
		return "", fmt.Errorf("api get download url, call json.Unmarshal fail, body: %s", string(info.Data))
	}
	decodedData, err := crypto.Decode(encodedData, key)
	if err != nil {
		return "", fmt.Errorf("api get download url, call Decode fail, err: %w", err)
	}

	result := api.DownloadData{}
	if err := json.Unmarshal(decodedData, &result); err != nil {
		return "", fmt.Errorf("api get download url, call json.Unmarshal fail, body: %s", string(decodedData))
	}

	for _, info := range result {
		fileSize, _ := info.FileSize.Int64()
		if fileSize == 0 {
			return "", fs.ErrorObjectNotFound
		}
		f.cache.SetDefault(cacheKey, info.URL.URL)
		return info.URL.URL, nil
	}

	return "", fs.ErrorObjectNotFound
}

func (f *Fs) indexInfo(ctx context.Context) (*api.IndexInfoResponse, error) {
	opts := rest.Opts{
		Method: http.MethodGet,
		Path:   "/files/index_info",
	}

	var err error
	var info api.IndexInfoResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return f.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}

	return &info, nil
}

func (f *Fs) createObject(remote string, modTime time.Time, size int64) *Object {
	return &Object{
		fs:      f,
		remote:  remote,
		size:    size,
		modTime: modTime,
	}
}

func (f *Fs) remotePath(name string) string {
	name = path.Join(f.root, name)
	if name == "" || name[0] != '/' {
		name = "/" + name
	}
	return path.Clean(name)
}

func (f *Fs) flushDir(dir string) {
	cacheKey := fmt.Sprintf("files:%v", path.Clean(dir))
	f.cache.Delete(cacheKey)
}

// ------------------------------------------------------------

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	return o.fs

}

// String returns a description of the Object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns the modification date of the file
// It should return a best guess if one isn't available
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	return o.size
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.SHA1 {
		return "", hash.ErrUnsupported
	}
	return o.sha1sum, nil
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	var resp *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		fs.Debugf(o.fs, "open %v, options: %v", o.remote, options)
		targetURL, err := o.fs.getURL(ctx, o.remote, o.pickCode)
		if err != nil {
			return o.fs.shouldRetry(ctx, resp, err)
		}
		fs.FixRangeOption(options, o.size)

		// Use a new client without cookies for direct download URL
		// The download URL is from CDN/OSS and doesn't need 115 cookies
		client := &http.Client{}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return o.fs.shouldRetry(ctx, resp, err)
		}

		// Use generic User-Agent to match 115driver/go-resty success behavior
		// The server might be blocking "115Browser" UA if other browser headers are missing.
		// Also inject the auth cookies manually.
		cookieStr := fmt.Sprintf("UID=%s; CID=%s; SEID=%s; KID=%s", o.fs.opt.UID, o.fs.opt.CID, o.fs.opt.SEID, o.fs.opt.KID)

		req.Header.Set("User-Agent", "go-resty/2.14.0 (https://github.com/go-resty/resty)")
		req.Header.Set("Cookie", cookieStr)
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded") // Copying from 115driver just in case

		// Apply range options - only set header if key is not empty
		for _, option := range options {
			key, value := option.Header()
			if key != "" && value != "" {
				req.Header.Set(key, value)
			}
		}

		resp, err = client.Do(req)
		return o.fs.shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, err
}

// Remove this object
func (o *Object) Remove(ctx context.Context) error {
	info, err := o.fs.readMetaDataForPath(ctx, o.fs.remotePath(o.Remote()))
	if err != nil {
		fs.Errorf(o.fs, "remove object fail, err: %v, remote: %v", err, o.remote)
		return err
	}

	if info.IsDir() {
		return fs.ErrorIsDir
	}

	err = o.fs.deleteFile(ctx, info.GetFileID(), info.GetCategoryID())
	if err != nil {
		return err
	}

	parent, _ := path.Split(o.fs.remotePath(o.remote))
	o.fs.flushDir(parent)

	return nil
}

// SetModTime sets the metadata on the object to set the modification date
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	o.modTime = modTime
	return nil
}

// Storable says whether this object can be stored
func (o *Object) Storable() bool {
	return true
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return strconv.FormatInt(o.fileID, 10)
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	f := o.fs
	if src.Size() == 0 {
		fs.Debugf(src, "Skipping upload of empty file (not supported by 115)")
		return nil
	}
	// TODO: enable file upload

	obj, err := f.NewObject(ctx, src.Remote())
	if err == nil {
		fs.Logf(f, "file exist, remote it, %+v", obj)
		err := obj.Remove(ctx)
		if err != nil {
			fs.Logf(f, "remove file fail, %+v", obj)
			return err
		}
	}

	// Use 115driver for upload
	// 115driver requires a robust io.ReadSeeker for hash calculation (multiple passes)
	// rclone might provide an asyncreader which fails Seek, so we must verify or spool.
	var uploadSeeker io.ReadSeeker
	var tempFile *os.File
	var seekableReader io.ReadCloser

	// Helper to cleanup temp file if used
	defer func() {
		if tempFile != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempFile.Name())
		}
		// If we opened a new reader from source, close it
		if seekableReader != nil {
			_ = seekableReader.Close()
		}
	}()

	isSeekable := false
	if seeker, ok := in.(io.ReadSeeker); ok {
		// Verify it actually supports seeking (AsyncReader panics/errors)
		if _, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			// Also need to be able to rewind to start
			if _, err := seeker.Seek(0, io.SeekStart); err == nil {
				isSeekable = true
				uploadSeeker = seeker
			}
		}
	}

	// Optimization: If input is not seekable, check if source object supports reopening as seekable
	if !isSeekable {
		if srcObj, ok := src.(fs.Object); ok {
			// Try to open the source object to see if we get a seekable reader (e.g. valid for local files)
			// This avoids spooling the file to a temp file if we can just re-read it from source
			rc, err := srcObj.Open(ctx)
			if err == nil {
				if seeker, ok := rc.(io.ReadSeeker); ok {
					// Verify seek support
					if _, err := seeker.Seek(0, io.SeekStart); err == nil {
						fs.Debugf(src, "Optimization: Using seekable source reader instead of spooling")
						isSeekable = true
						uploadSeeker = seeker
						seekableReader = rc // Keep reference to close later
					} else {
						_ = rc.Close()
					}
				} else {
					_ = rc.Close()
				}
			}
		}
	}

	if !isSeekable {
		// Spool to temporary file
		fs.Logf(f, "Input is not seekable, spooling to temporary file for 115 upload")
		var err error
		tempFile, err = os.CreateTemp("", "rclone-115-upload-*")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}

		if _, err := io.Copy(tempFile, in); err != nil {
			return fmt.Errorf("failed to spool to temp file: %w", err)
		}

		// Rewind temp file
		if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to rewind temp file: %w", err)
		}
		uploadSeeker = tempFile
	}

	dir, name := path.Split(f.remotePath(src.Remote()))
	// We need the directory ID (cid) for 115driver
	// note: using dir as string ID might be tricky if it expects root ID etc.
	// 115driver usually takes "cid" string.
	cid, err := f.getDirID(ctx, dir)
	if err != nil {
		return err
	}
	cidStr := strconv.FormatInt(cid, 10)

	if err := f.client.UploadFastOrByOSS(cidStr, name, src.Size(), uploadSeeker); err != nil {
		return fmt.Errorf("115driver upload failed: %w", err)
	}

	f.flushDir(dir)
	o.hasMetaData = false
	return o.readMetaData(ctx)
}

// setMetaData sets the metadata from info
func (o *Object) setMetaData(info *api.FileInfo) error {
	o.hasMetaData = true
	o.name = o.fs.opt.Enc.ToStandardName(info.GetName())
	o.size = info.GetSize()
	o.sha1sum = strings.ToLower(info.Sha1)
	o.pickCode = info.PickCode
	o.fileID = info.GetFileID()
	o.modTime = info.GetUpdateTime()
	return nil
}

// readMetaData gets the metadata if it hasn't already been fetched
func (o *Object) readMetaData(ctx context.Context) error {
	if o.hasMetaData {
		return nil
	}

	info, err := o.fs.readMetaDataForPath(ctx, o.fs.remotePath(o.Remote()))
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fs.ErrorIsDir
	}

	return o.setMetaData(info)
}

// Check the interfaces are satisfied
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Purger = (*Fs)(nil)
	// _ fs.Copier       = (*Fs)(nil)
	_ fs.Mover    = (*Fs)(nil)
	_ fs.DirMover = (*Fs)(nil)
	// _ fs.PublicLinker = (*Fs)(nil)
	// _ fs.CleanUpper   = (*Fs)(nil)
	_ fs.Abouter    = (*Fs)(nil)
	_ fs.Object     = (*Object)(nil)
	_ fs.ObjectInfo = (*Object)(nil)
	_ fs.IDer       = (*Object)(nil)
	// _ fs.MimeTyper = (*Object)(nil)
)
