package nsqd

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/bitly/nsq/internal/http_api"
	"github.com/bitly/nsq/internal/lookupd"
	"github.com/bitly/nsq/internal/protocol"
	"github.com/bitly/nsq/internal/statsd"
	"github.com/bitly/nsq/internal/util"
	"github.com/bitly/nsq/internal/version"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

const (
	flagHealthy = 1 << iota
	flagLoading
)

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64

	sync.RWMutex //匿名组合

	opts *nsqdOptions

	flag      int32
	errMtx    sync.RWMutex
	err       error
	startTime time.Time

	topicMap map[string]*Topic

	lookupPeers []*lookupPeer

	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	tlsConfig     *tls.Config

	poolSize int

	idChan     chan MessageID
	notifyChan chan interface{}
	exitChan   chan int
	waitGroup  util.WaitGroupWrapper
}

func NewNSQD(opts *nsqdOptions) *NSQD {
	n := &NSQD{
		opts:       opts,
		flag:       flagHealthy,
		startTime:  time.Now(),
		topicMap:   make(map[string]*Topic),
		idChan:     make(chan MessageID, 4096),
		exitChan:   make(chan int),
		notifyChan: make(chan interface{}),
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		n.logf("FATAL: --max-deflate-level must be [1,9]")
		os.Exit(1)
	}

	/*
	   范围由以下代码决定:

	   bitly\nsq\nsqd\options.go:88
	   defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)
	*/
	if opts.ID < 0 || opts.ID >= 1024 {
		n.logf("FATAL: --worker-id must be [0,1024)")
		os.Exit(1)
	}

	if opts.StatsdPrefix != "" {
		_, port, err := net.SplitHostPort(opts.HTTPAddress)
		if err != nil {
			n.logf("ERROR: failed to parse HTTP address (%s) - %s", opts.HTTPAddress, err)
			os.Exit(1)
		}
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		n.logf("FATAL: failed to build TLS config - %s", err)
		os.Exit(1)
	}
	if tlsConfig == nil && n.opts.TLSRequired != TLSNotRequired {
		n.logf("FATAL: cannot require TLS client connections without TLS key and cert")
		os.Exit(1)
	}
	n.tlsConfig = tlsConfig

	n.logf(version.String("nsqd"))
	n.logf("ID: %d", n.opts.ID)

	return n
}

func (n *NSQD) logf(f string, args ...interface{}) {
	if n.opts.Logger == nil {
		return
	}
	n.opts.Logger.Output(2, fmt.Sprintf(f, args...))
}

func (n *NSQD) RealTCPAddr() *net.TCPAddr {
	n.RLock()
	defer n.RUnlock()
	return n.tcpListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) RealHTTPAddr() *net.TCPAddr {
	n.RLock()
	defer n.RUnlock()
	return n.httpListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) setFlag(f int32, b bool) {
	for {
		old := atomic.LoadInt32(&n.flag)
		newFlag := old
		if b {
			newFlag |= f
		} else {
			newFlag &= ^f
		}
		if atomic.CompareAndSwapInt32(&n.flag, old, newFlag) {
			return
		}
	}
}

func (n *NSQD) getFlag(f int32) bool {
	return f&atomic.LoadInt32(&n.flag) != 0
}

func (n *NSQD) SetHealth(err error) {
	n.errMtx.Lock()
	defer n.errMtx.Unlock()
	n.err = err
	if err != nil {
		n.setFlag(flagHealthy, false)
	} else {
		n.setFlag(flagHealthy, true)
	}
}

func (n *NSQD) IsHealthy() bool {
	return n.getFlag(flagHealthy)
}

func (n *NSQD) GetError() error {
	n.errMtx.RLock()
	defer n.errMtx.RUnlock()
	return n.err
}

func (n *NSQD) GetHealth() string {
	if !n.IsHealthy() {
		return fmt.Sprintf("NOK - %s", n.GetError())
	}
	return "OK"
}

func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

func (n *NSQD) Main() {
	var httpListener net.Listener
	var httpsListener net.Listener

	ctx := &context{n}

	n.opts.Logger.Output(2, "Starting net.Listen(tcp)\n")
	tcpListener, err := net.Listen("tcp", n.opts.TCPAddress)
	if err != nil {
		n.logf("FATAL: listen (%s) failed - %s", n.opts.TCPAddress, err)
		os.Exit(1)
	}
	n.Lock()
	n.tcpListener = tcpListener
	n.Unlock()
	tcpServer := &tcpServer{ctx: ctx}
	n.waitGroup.Wrap(func() {
		n.opts.Logger.Output(2, "Starting protocol.TCPServer()\n")
		protocol.TCPServer(n.tcpListener, tcpServer, n.opts.Logger)
	})

	if n.tlsConfig != nil && n.opts.HTTPSAddress != "" {
		httpsListener, err = tls.Listen("tcp", n.opts.HTTPSAddress, n.tlsConfig)
		if err != nil {
			n.logf("FATAL: listen (%s) failed - %s", n.opts.HTTPSAddress, err)
			os.Exit(1)
		}
		n.Lock()
		n.httpsListener = httpsListener
		n.Unlock()
		httpsServer := &httpServer{
			ctx:         ctx,
			tlsEnabled:  true,
			tlsRequired: true,
		}
		n.waitGroup.Wrap(func() {
			http_api.Serve(n.httpsListener, httpsServer, n.opts.Logger, "HTTPS")
		})
	}

	httpListener, err = net.Listen("tcp", n.opts.HTTPAddress)
	if err != nil {
		n.logf("FATAL: listen (%s) failed - %s", n.opts.HTTPAddress, err)
		os.Exit(1)
	}
	n.Lock()
	n.httpListener = httpListener
	n.Unlock()

	//创建新的自定义httpServer
	httpServer := &httpServer{
		ctx:         ctx,
		tlsEnabled:  false,
		tlsRequired: n.opts.TLSRequired == TLSRequired,
	}
	n.waitGroup.Wrap(func() {
		n.opts.Logger.Output(2, "Starting http_api.Serve()\n")
		http_api.Serve(n.httpListener, httpServer, n.opts.Logger, "HTTP")
	})

	n.waitGroup.Wrap(func() { n.queueScanLoop() })
	n.waitGroup.Wrap(func() { n.idPump() })
	n.waitGroup.Wrap(func() { n.lookupLoop() })
	if n.opts.StatsdAddress != "" {
		n.waitGroup.Wrap(func() { n.statsdLoop() })
	}
}

func (n *NSQD) LoadMetadata() {
	n.setFlag(flagLoading, true)
	defer n.setFlag(flagLoading, false)
	fn := fmt.Sprintf(path.Join(n.opts.DataPath, "nsqd.%d.dat"), n.opts.ID)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			n.logf("ERROR: failed to read channel metadata from %s - %s", fn, err)
		}
		return
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		n.logf("ERROR: failed to parse metadata - %s", err)
		return
	}

	topics, err := js.Get("topics").Array()
	if err != nil {
		n.logf("ERROR: failed to parse metadata - %s", err)
		return
	}

	for ti := range topics {
		topicJs := js.Get("topics").GetIndex(ti)

		topicName, err := topicJs.Get("name").String()
		if err != nil {
			n.logf("ERROR: failed to parse metadata - %s", err)
			return
		}
		if !protocol.IsValidTopicName(topicName) {
			n.logf("WARNING: skipping creation of invalid topic %s", topicName)
			continue
		}
		topic := n.GetTopic(topicName)

		paused, _ := topicJs.Get("paused").Bool()
		if paused {
			topic.Pause()
		}

		channels, err := topicJs.Get("channels").Array()
		if err != nil {
			n.logf("ERROR: failed to parse metadata - %s", err)
			return
		}

		for ci := range channels {
			channelJs := topicJs.Get("channels").GetIndex(ci)

			channelName, err := channelJs.Get("name").String()
			if err != nil {
				n.logf("ERROR: failed to parse metadata - %s", err)
				return
			}
			if !protocol.IsValidChannelName(channelName) {
				n.logf("WARNING: skipping creation of invalid channel %s", channelName)
				continue
			}
			channel := topic.GetChannel(channelName)

			paused, _ = channelJs.Get("paused").Bool()
			if paused {
				channel.Pause()
			}
		}
	}
}

func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fileName := fmt.Sprintf(path.Join(n.opts.DataPath, "nsqd.%d.dat"), n.opts.ID)
	n.logf("NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := []interface{}{}
	for _, topic := range n.topicMap {
		if topic.ephemeral {
			continue
		}
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := []interface{}{}
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if channel.ephemeral {
				channel.Unlock()
				continue
			}
			channelData := make(map[string]interface{})
			channelData["name"] = channel.name
			channelData["paused"] = channel.IsPaused()
			channels = append(channels, channelData)
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = version.Binary
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	err = atomicRename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}

func (n *NSQD) Exit() {
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		n.httpsListener.Close()
	}

	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf("ERROR: failed to persist metadata - %s", err)
	}
	n.logf("NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(n.exitChan)
	n.waitGroup.Wait()
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *NSQD) GetTopic(topicName string) *Topic {

	n.Lock()
	//尝试从topicMap中获取指定名称的Topic对象
	t, ok := n.topicMap[topicName]
	if ok {
		//找到了，说明之前已经创建的Topic
		fmt.Printf("nsqd.GetTopic() find name [%s] specified topic from map\n", topicName)
		n.Unlock()
		return t
	}
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	t = NewTopic(topicName, &context{n}, deleteCallback)
	n.topicMap[topicName] = t

	n.logf("TOPIC(%s): created", t.name)

	// release our global nsqd lock, and switch to a more granular topic lock while we init our
	// channels from lookupd. This blocks concurrent PutMessages to this topic.
	t.Lock()
	n.Unlock()
	// if using lookupd, make a blocking call to get the topics, and immediately create them.
	// this makes sure that any message received is buffered to the right channels
	n.logf("the len of n.LookupPeers :%d\n", len(n.lookupPeers))
	if len(n.lookupPeers) > 0 {
		channelNames, _ := lookupd.GetLookupdTopicChannels(t.name, n.lookupHTTPAddrs())
		for _, channelName := range channelNames {
			if strings.HasSuffix(channelName, "#ephemeral") {
				// we don't want to pre-create ephemeral channels
				// because there isn't a client connected
				continue
			}
			t.getOrCreateChannel(channelName)
		}
	}
	t.Unlock()

	// NOTE: I would prefer for this to only happen in topic.GetChannel() but we're special
	// casing the code above so that we can control the locks such that it is impossible
	// for a message to be written to a (new) topic while we're looking up channels
	// from lookupd...
	//
	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

func (n *NSQD) idPump() {
	factory := &guidFactory{}
	lastError := time.Unix(0, 0)
	for {
		id, err := factory.NewGUID(n.opts.ID)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				n.logf("ERROR: %s", err)
				lastError = now
			}
			runtime.Gosched()
			continue
		}
		select {
		case n.idChan <- id.Hex():
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf("ID: closing")
}

func (n *NSQD) Notify(v interface{}) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	persist := !n.getFlag(flagLoading)
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.notifyChan <- v:
			if !persist {
				return
			}
			n.Lock()
			err := n.PersistMetadata()
			if err != nil {
				n.logf("ERROR: failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// channels returns a flat slice of all channels in all topics
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
//
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.opts.QueueScanWorkerPoolMax {
		idealPoolSize = n.opts.QueueScanWorkerPoolMax
	}
	for {
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize {
			// contract
			closeCh <- 1
			n.poolSize--
		} else {
			// expand
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			if c.processInFlightQueue(now) {
				dirty = true
			}
			if c.processDeferredQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
func (n *NSQD) queueScanLoop() {
	fmt.Printf("nsqd.queueScanLoop() Starting\n")
	workCh := make(chan *Channel, n.opts.QueueScanSelectionCount)
	responseCh := make(chan bool, n.opts.QueueScanSelectionCount)
	closeCh := make(chan int)

	//两个Ticker
	workTicker := time.NewTicker(n.opts.QueueScanInterval)
	refreshTicker := time.NewTicker(n.opts.QueueScanRefreshInterval)

	channels := n.channels()

	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan:
			goto exit
		}

		num := n.opts.QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}
	loop:
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		if float64(numDirty)/float64(num) > n.opts.QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	n.logf("QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

func buildTLSConfig(opts *nsqdOptions) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
		MaxVersion:   tls.VersionTLS12, // enable TLS_FALLBACK_SCSV prior to Go 1.5: https://go-review.googlesource.com/#/c/1776/
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.opts.AuthHTTPAddresses) != 0
}
