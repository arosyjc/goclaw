package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// ===================== 全局常量配置（可根据业务调整） =====================
const (
	// 服务配置
	httpAddr        = ":8080"
	maxConnNum      = 1000 // 全局最大并发连接数
	writeChanBuf    = 256  // 单连接发送队列缓冲大小
	maxReadMsgSize  = 4096 // 单条消息最大字节，防超大包攻击
	heartbeatPeriod = 30 * time.Second
	readDeadline    = 60 * time.Second // 读超时，心跳超时触发关闭
	writeDeadline   = 10 * time.Second // 写超时

	// 消息类型
	MsgTypeText   = 1
	MsgTypeBinary = 2
	MsgTypePing   = 9
	MsgTypePong   = 10
)

// ===================== 消息结构体 =====================
type Message struct {
	MsgType int         `json:"msg_type"`
	Data    interface{} `json:"data"`
}

// ===================== 全局连接管理器（并发安全） =====================
type ConnManager struct {
	mu        sync.RWMutex
	conns     map[*WsConn]struct{}
	current   int           // 当前连接数
	max       int           // 最大允许连接
	closeChan chan struct{} // 全局关闭信号
}

func NewConnManager(max int) *ConnManager {
	return &ConnManager{
		conns:     make(map[*WsConn]struct{}),
		max:       max,
		closeChan: make(chan struct{}),
	}
}

// Add 新增连接，超出上限返回拒绝
func (m *ConnManager) Add(conn *WsConn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current >= m.max {
		return errors.New("too many connections, server busy")
	}
	m.conns[conn] = struct{}{}
	m.current++
	return nil
}

// Remove 移除连接
func (m *ConnManager) Remove(conn *WsConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conns, conn)
	m.current--
}

// GetAll 获取全部连接（读锁，不阻塞写入）
func (m *ConnManager) GetAll() []*WsConn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := make([]*WsConn, 0, len(m.conns))
	for c := range m.conns {
		list = append(list, c)
	}
	return list
}

// CloseAll 优雅关闭所有连接
func (m *ConnManager) CloseAll() {
	close(m.closeChan)
	conns := m.GetAll()
	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Add(1)
		go func(conn *WsConn) {
			defer wg.Done()
			conn.Close()
		}(c)
	}
	wg.Wait()
	log.Println("all websocket connections closed")
}

// ===================== 单个WebSocket连接封装 =====================
type WsConn struct {
	ctx        context.Context
	cancel     context.CancelFunc
	ws         *websocket.Conn
	writeChan  chan *Message
	manager    *ConnManager
	closeOnce  sync.Once // 保证Close只执行一次，防重复关闭panic
}

func NewWsConn(ws *websocket.Conn, manager *ConnManager) *WsConn {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &WsConn{
		ctx:       ctx,
		cancel:    cancel,
		ws:        ws,
		writeChan: make(chan *Message, writeChanBuf),
		manager:   manager,
	}
	// 设置ping/pong回调，自动刷新读超时
	ws.SetPongHandler(func(string) error {
		_ = ws.SetReadDeadline(time.Now().Add(readDeadline))
		return nil
	})
	return conn
}

// Start 启动读写双协程
func (c *WsConn) Start() {
	// 读协程：接收客户端消息
	go c.readLoop()
	// 写协程：异步发送消息
	go c.writeLoop()
	// 心跳协程：定时发送ping保活
	go c.heartbeatLoop()
}

// readLoop 循环读取客户端消息，所有panic recover
func (c *WsConn) readLoop() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("read loop panic recovered: %v", err)
		}
		c.Close()
	}()

	ws := c.ws
	ws.SetReadLimit(maxReadMsgSize)
	_ = ws.SetReadDeadline(time.Now().Add(readDeadline))

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		msgType, data, err := ws.ReadMessage()
		if err != nil {
			var closeErr *websocket.CloseError
			if errors.As(err, &closeErr) {
				log.Printf("client close, code=%d, msg=%s", closeErr.Code, closeErr.Text)
			} else if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("unexpected read error: %v", err)
			} else {
				log.Printf("read message err: %v", err)
			}
			return
		}

		// 业务消息处理，这里可替换成自定义业务逻辑
		c.handleMsg(msgType, data)
	}
}

// writeLoop 异步写队列，防止发送阻塞读
func (c *WsConn) writeLoop() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("write loop panic recovered: %v", err)
		}
		c.Close()
	}()

	ws := c.ws
	for {
		select {
		case <-c.ctx.Done():
			// 退出前发送关闭帧
			_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "server exit"), time.Now().Add(writeDeadline))
			return
		case msg, ok := <-c.writeChan:
			if !ok {
				return
			}
			_ = ws.SetWriteDeadline(time.Now().Add(writeDeadline))
			var err error
			switch msg.MsgType {
			case MsgTypeText:
				buf, e := json.Marshal(msg)
				if e != nil {
					log.Printf("json marshal err: %v", e)
					continue
				}
				err = ws.WriteMessage(websocket.TextMessage, buf)
			case MsgTypeBinary:
				err = ws.WriteMessage(websocket.BinaryMessage, msg.Data.([]byte))
			case MsgTypePing:
				err = ws.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(writeDeadline))
			}
			if err != nil {
				log.Printf("write message err: %v", err)
				return
			}
		}
	}
}

// heartbeatLoop 定时发送ping包保活
func (c *WsConn) heartbeatLoop() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("heartbeat panic recovered: %v", err)
		}
	}()

	ticker := time.NewTicker(heartbeatPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			// 发送ping
			c.Send(&Message{MsgType: MsgTypePing})
		}
	}
}

// Send 对外发送消息，非阻塞写入队列，队列满则丢弃并告警
func (c *WsConn) Send(msg *Message) bool {
	select {
	case c.writeChan <- msg:
		return true
	default:
		log.Println("write channel full, drop message")
		return false
	}
}

// Close 安全关闭连接，只执行一次
func (c *WsConn) Close() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.manager.Remove(c)
		_ = c.ws.Close()
		close(c.writeChan)
		log.Println("websocket connection closed")
	})
}

// handleMsg 业务消息处理示例
func (c *WsConn) handleMsg(msgType int, data []byte) {
	log.Printf("recv msg, type=%d, len=%d", msgType, len(data))
	// 回显消息示例
	var echo map[string]interface{}
	if err := json.Unmarshal(data, &echo); err == nil {
		resp := Message{
			MsgType: MsgTypeText,
			Data: map[string]interface{}{
				"code": 0,
				"msg":  "echo success",
				"data": echo,
			},
		}
		c.Send(&resp)
	}
}

// ===================== HTTP 升级WS处理器 =====================
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// 生产环境务必替换为固定域名，禁止 *
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		// 示例：仅允许前端域名，测试可临时放行
		return origin == "http://127.0.0.1:3000" || origin == ""
	},
}

var manager *ConnManager

func wsHandler(w http.ResponseWriter, r *http.Request) {
	// 升级HTTP到WebSocket
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade ws failed: %v", err)
		http.Error(w, "upgrade failed", http.StatusBadRequest)
		return
	}

	// 新建连接，校验连接上限
	conn := NewWsConn(ws, manager)
	if err := manager.Add(conn); err != nil {
		log.Printf("reject connection: %v", err)
		_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, err.Error()), time.Now().Add(writeDeadline))
		_ = ws.Close()
		return
	}

	log.Printf("new client connect, total=%d", manager.current)
	conn.Start()
}

// broadcast 全局广播示例：给所有连接发消息
func broadcast(msg *Message) {
	for _, conn := range manager.GetAll() {
		conn.Send(msg)
	}
}

func main() {
	// 初始化连接管理器
	manager = NewConnManager(maxConnNum)

	// 注册路由
	http.HandleFunc("/ws", wsHandler)

	// 启动HTTP服务
	srv := &http.Server{
		Addr:         httpAddr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// 协程启动服务
	go func() {
		log.Printf("websocket server listen on %s", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server start failed: %v", err)
		}
	}()

	// 捕获系统信号，优雅退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("receive shutdown signal, start graceful exit...")

	// 1. 停止接收新HTTP连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http server shutdown err: %v", err)
	}

	// 2. 关闭所有WS连接
	manager.CloseAll()
	log.Println("server exit complete")
}