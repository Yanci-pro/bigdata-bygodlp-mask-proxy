package main

import (
	"fmt"
	"net"
	"strings"
	"sync"

	dlp "github.com/bytedance/godlp"
)

type ConnInfo struct {
	//数据库类型：预留字段
	dbType string
	//会话的pid：预留字段
	sessionId string
	//发送的连接
	sendConn net.Conn
	//接收数据的连接
	receConn net.Conn
	//当前信息状态：预留字段
	status int
}

var (
	wg     sync.WaitGroup
	split  string
	splitB = []byte{13, 10}
)

func start() {
	wg.Add(1)
	split = string(splitB)
	//与客户端的连接,其实是客户端需要连接的服务端
	cliAdd, _ := net.ResolveTCPAddr("tcp4", "172.16.x.x:6666")
	cliListen, _ := net.ListenTCP("tcp4", cliAdd)

	fmt.Println("代理工具启动监听端口:", cliAdd)
	//无限循环，接收到来自于客户端的连接的时候创建一个函数去处理，暂时用chan通道去处理
	chConn := make(chan ConnInfo)
	go func() {
		fmt.Println("准备开始监听通道")
		dealChannel(chConn)
	}()

	for {
		conn, _ := cliListen.Accept()
		fmt.Println("接收到新请求")
		//这里接收到一个客户端请求则直接创建一个与服务端相连接的请求
		serAdd, _ := net.ResolveTCPAddr("tcp4", "172.16.x.x:6379")
		serConn, _ := net.DialTCP("tcp4", nil, serAdd)
		//封装服务端的连接和客户端的连接对象建立关系
		serConnInfo := new(ConnInfo)
		serConnInfo.sessionId = "readcli"
		serConnInfo.receConn = conn
		serConnInfo.sendConn = serConn
		cliConnInfo := new(ConnInfo)
		cliConnInfo.sessionId = "readser"

		cliConnInfo.receConn = serConn
		cliConnInfo.sendConn = conn
		chConn <- *serConnInfo
		chConn <- *cliConnInfo
	}
}

func dealChannel(chConn chan ConnInfo) {
	//这里无限循环一直从通道中去获取连接信息，目前这种处理方式可能并发性能差点，待研究深入一点再考虑更好的方案
	for {
		conInfo := <-chConn
		//当从通道读取到连接信息的时候创建一个协程去读取数据，这样会一直创建协程，因为go语言的协程本来久很轻量级，不确定这么处理是否有问题，待后续深入了解了再改进
		go func() {
			//获取到通道的信息
			data := make([]byte, 1024)
			dlen, err := conInfo.receConn.Read(data)
			if err != nil {
				//如果出错大概率是因为某个通道被断开了，会话断开直接连接，通道中丢掉这个连接信息
				fmt.Print("出现错误")
				conInfo.receConn.Close()
				conInfo.sendConn.Close()
			} else {
				//这里直接取了所有数据，如果返回的数据量大可能会分包，暂时不处理后续再想办法。
				content := strings.Split(string(data[:dlen]), split)
				for i, value := range content {
					if strings.HasPrefix(value, "*") || strings.HasPrefix(value, "$") {
						conInfo.sendConn.Write([]byte(value))
					} else {
						conInfo.sendConn.Write([]byte(maskString(value)))
					}
					if i < len(content)-1 {
						conInfo.sendConn.Write(splitB)
					}
				}
				//读取结束再把信息加入通道，等待下次循环再次读取
				chConn <- conInfo
			}
		}()
	}
}

//这里直接把数据交给godlp处理，至于能识别到什么内容需要看godlp只是什么，当然以前的项目是自定义的也没问题
func maskString(inStr string) (outStr string) {
	caller := "replace.your.caller"
	if eng, err := dlp.NewEngine(caller); err == nil {
		eng.ApplyConfigDefault()
		if outStr, _, err := eng.Deidentify(inStr); err == nil {
			//fmt.Println(inStr, "--------->", outStr)
			return outStr
		}
		eng.Close()
	} else {
		fmt.Println("[dlp] NewEngine error: ", err.Error())
	}
	return inStr
}

//启动的主方法
func main() {
	start()
}
