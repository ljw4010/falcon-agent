package g

import (
	"math/rand"
	"strings"
	"time"

	"encoding/binary"
	"encoding/json"

	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"unsafe"

	"github.com/open-falcon/falcon-plus/common/model"
)

func SendZabbixMetrics(metrics []*model.MetricValue) {
	rand.Seed(time.Now().UnixNano())
	wg := sync.WaitGroup{}
	for _, i := range rand.Perm(len(Config().Zabbix.Addrs)) {
		wg.Add(1)
		addr := Config().Zabbix.Addrs[i]

		tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal error: %s\n", err.Error())
		}

		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		}

		err = conn.SetDeadline(time.Now().Add(time.Duration(Config().Zabbix.Timeout) * time.Second))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Conn timeout error: %s", err.Error())
		}

		defer conn.Close()
		log.Printf("connect zabbix server[%s] success", addr)
		DoSender(conn, metrics, &wg)
	}
	wg.Wait()
	log.Printf("send all zabbix server success")
}

//封包
func Packet(message []byte) []byte {
	return append(append(append([]byte(ConstHeader),
		IntToBytes(ZabbixVersion, 4)...),
		IntToBytes(len(message), 8)...), message...)
}

//整形转换成字节
func IntToBytes(n, x int) []byte {

	// 小端处理
	b := make([]byte, x)
	i := uint32(n)
	binary.LittleEndian.PutUint32(b, i)
	if x == 4 {
		var bx []byte
		bx = append(bx, b[0])
		return bx
	}
	return b
}

// 是否小端
func IsLittleEndian() bool {
	var i int32 = 0x01020304
	u := unsafe.Pointer(&i)
	pb := (*byte)(u)
	b := *pb
	return (b == 0x04)
}

// 请求协议
type ReqPt struct {
	Data    []map[string]string `json:"data"`
	Request string              `json:"request"`
}

func DoSender(conn net.Conn, metrics []*model.MetricValue, wg *sync.WaitGroup) {
	defer wg.Done()
	var datas []map[string]string
	for _, m := range metrics {
		var data map[string]string
		data = make(map[string]string, 0)
		data["host"] = m.Endpoint
		if strings.EqualFold(m.Tags, "") {
			data["key"] = fmt.Sprintf("%v", m.Metric)
		} else {
			data["key"] = fmt.Sprintf("%v[%v]", m.Metric, m.Tags)
		}
		data["value"] = fmt.Sprintf("%v", m.Value)
		datas = append(datas, data)
	}
	reqData := ReqPt{
		Data:    datas,
		Request: "sender data",
	}
	reqDataSlice, _ := json.Marshal(reqData)
	_, err := conn.Write(Packet(reqDataSlice))
	if err != nil {
		log.Printf("bad packet: %v", err)
	}

	bys, err := ioutil.ReadAll(conn)
	if err != nil {
		log.Printf("read response failed: %v", err)
	}
	var reciveText string = ""
	if len(bys) > 13 {
		reciveText = string(bys[13:])
	} else {
		reciveText = string(bys)
	}

	log.Printf("send zabbix[%s] success,resp:[%+v]", conn.RemoteAddr(), reciveText)
}
