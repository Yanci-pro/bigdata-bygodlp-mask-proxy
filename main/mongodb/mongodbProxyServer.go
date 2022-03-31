package main

import (
	"bytes"
	"fmt"
	"net"
	"sync"

	dlp "github.com/bytedance/godlp"
)

type ConnInfo struct {
	//数据库类型：预留字段
	dbType string
	//会话的pid：预留字段
	sessionId string
	//连接的type
	conType string
	//发送的连接
	sendConn net.Conn
	//接收数据的连接
	receConn net.Conn
	//当前信息状态
	status int
	//当前数据是否接收完整,默认是false
	//接收的buffer
	rebuffer bytes.Buffer
	//发送的buffer,预留属性,后续可能会用到
	sebuffer bytes.Buffer
}

var (
	wg sync.WaitGroup
)

func main() {
	start()
}

/**
*这里只写了点对点代理，完全可以多加循环做成用不同端口代理多个服务器
 */
func start() {
	wg.Add(1)
	//与客户端的连接,其实是客户端需要连接的服务端
	cliAdd, _ := net.ResolveTCPAddr("tcp4", "127.0.0.1:7002")
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
		serAdd, _ := net.ResolveTCPAddr("tcp4", "172.x.x.x:27017")
		serConn, _ := net.DialTCP("tcp4", nil, serAdd)
		//封装服务端的连接和客户端的连接对象建立关系
		serConnInfo := new(ConnInfo)
		serConnInfo.conType = "mogcli"
		serConnInfo.receConn = conn
		serConnInfo.status = 0
		serConnInfo.sendConn = serConn
		cliConnInfo := new(ConnInfo)
		cliConnInfo.conType = "mogser"

		cliConnInfo.receConn = serConn
		cliConnInfo.sendConn = conn
		cliConnInfo.status = 0
		chConn <- *serConnInfo
		chConn <- *cliConnInfo
	}
}

/**
处理channel通道的数据
*/
func dealChannel(chConn chan ConnInfo) {
	//这里无限循环一直从通道中去获取连接信息，目前这种处理方式可能并发性能差点，待研究深入一点再考虑更好的方案
	for {
		conInfo := <-chConn
		//当从通道读取到连接信息的时候创建一个协程去读取数据，这样会一直创建协程，因为go语言的协程本来久很轻量级，不确定这么处理是否有问题，待后续深入了解了再改进
		go func() {
			dealConnInfo(conInfo, chConn)
		}()
	}
}

//对接收到的信息再进行分析处理
func dealConnInfo(conInfo ConnInfo, chConn chan ConnInfo) {
	//获取到通道的信息
	datas := make([]byte, 1024)
	dlen, err := conInfo.receConn.Read(datas)

	if err != nil {
		//如果出错大概率是因为某个通道被断开了，会话断开直接连接，通道中丢掉这个连接信息
		fmt.Print("出现错误")
		conInfo.receConn.Close()
		conInfo.sendConn.Close()
	} else if conInfo.conType == "mogser" {
		//1.判断当前的数据包是否完整,如果数据包接收不完整就存入通道的连接对象中去，等数据接收完毕再进行解析处理后统一转发给客户端。
		//这里这种方式存在风险，当返回的数据量足够多的话这样一直把所有数据存入到buffer中去肯定会出现问题，本来处理大量的内容就比较慢了，还要把这些内容存储在缓存里肯定是对机器资源耗费的很多的，
		//这里需要自己分包去解析处理的，这里为了方便就暂时先这么玩了，也留一点思考的空间给大家自己扩展
		//说明当前数据包中没有数据,需要等待
		if conInfo.rebuffer.Len() == 0 {
			//获取整体数据包的长度
			allPkLen := getBytesLen(datas, 0)
			if allPkLen == int64(dlen) {
				result := dealDatas(datas[:dlen])
				conInfo.sendConn.Write(result)
				conInfo.rebuffer.Reset()
			} else {
				conInfo.rebuffer.Write(datas[:dlen])
			}
		} else {
			buffer := conInfo.rebuffer
			buffer.Write(datas[:dlen])
			reDatas := buffer.Bytes()
			allPkLen := getBytesLen(reDatas, 0)
			if allPkLen == int64(buffer.Len()) {
				result := dealDatas(reDatas)
				conInfo.sendConn.Write(result)
				conInfo.rebuffer.Reset()
			} else {
				conInfo.rebuffer.Write(datas[:dlen])
			}
		}
		chConn <- conInfo
	} else {
		//这里直接取了所有数据，如果返回的数据量大可能会分包，暂时不处理后续再想办法。
		conInfo.sendConn.Write(datas[:dlen])
		//读取结束再把信息加入通道，等待下次循环再次读取
		chConn <- conInfo
	}
}

/**
 *对读取到的整体数据包进行解析处理,初学阶段就写流水帐了方便理解了，后续熟悉了再考虑用其他工具来封装简化处理。
 */
func dealDatas(datas []byte) (result []byte) {
	var position int64 = 26
	strTag, position := readBytesStr(datas, position)

	//说明当前收到的报文是数据接收报文
	if strTag == "cursor" {
		//读取数据包长度
		allPkLen := getBytesLen(datas, 0)
		//如果当前数据包长度等于整体数据包长度说明数据包接收完毕，执行正确的流程
		if allPkLen == int64(len(datas)) {
			//这里直接从49位开始读取，别问为啥，多读一下源码就知道了
			//先读取数据包长度
			dataPkLen := getBytesLen(datas, 49)
			//说明这里面有数据，需要进行脱敏处理
			position = 53
			endposition := position + dataPkLen
			if dataPkLen > 4 {
				for true {
					b := datas[position]
					position++
					if b == 0 || (int64(position) >= endposition) {
						break
					}
					position += 23
					for true {
						tag := datas[position]
						position++
						if tag == 0 {
							break
						} else if tag == 2 {
							_, dlen := readBytesStr(datas, position)
							position = dlen
							//读取这个value的数据包长度
							vlen := getBytesLen(datas, position)
							position += 4
							//读取value的值
							dealValueBytesStr(datas, position, vlen)
							position += vlen
						} else {
							// fmt.Println("读取到其他类型数据了")
						}

					}
				}
			}
		}
	}
	return datas
}

/**
 *根据起始位获取数据包的长度
 */
func getBytesLen(datas []byte, position int64) (len int64) {
	b0 := int64(datas[position])
	b1 := int64(datas[position+1]) << 8
	b2 := int64(datas[position+2]) << 16
	b3 := int64(datas[position+3]) << 24
	return b3 + b2 + b1 + b0
}

/**
 *处理数据把value读取处理后脱敏然后再把读取后的数据写入字byte种
 */
func dealValueBytesStr(datas []byte, position int64, vlen int64) {
	value := string(datas[position : position+vlen-1])
	nValue := maskValue(value)
	nByte := []byte(nValue)
	for i := int64(0); i < vlen-1; i++ {
		if i < int64(len(nByte)) {
			datas[position+i] = nByte[i]
		} else {
			datas[position+i] = 32
		}
	}
}

//脱敏传入的字符串并且返回脱敏后的结果,这里用godlp实现，所有的识别及脱敏算法全都用godlp的开源内容，当然也可以自己写或者扩展
func maskValue(inStr string) (res string) {
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

/**
 *从当前位置读取数据一直读取到下一位为0的时候结束
 */
func readBytesStr(datas []byte, position int64) (str string, dlen int64) {
	buffer := new(bytes.Buffer)
	for true {
		b := datas[position]
		position++
		if b == 0 {
			break
		}
		buffer.WriteByte(b)
	}
	str = buffer.String()
	return str, position
}
