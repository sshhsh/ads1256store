package main

import (
	"encoding/binary"
	"flag"
	"github.com/influxdata/influxdb1-client/v2"
	"log"
	"net"
	"strconv"
	"time"
)

func main() {
	var localAddr, influxAddress, userName, pass string
	flag.StringVar(&localAddr, "l", "0.0.0.0:1324", "local udp listener address")
	flag.StringVar(&influxAddress, "address", "http://127.0.0.1:8086", "InfluxDB address.")
	flag.StringVar(&userName, "user", "thinktank", "User name.")
	flag.StringVar(&pass, "pass", "thinktank", "Password.")
	flag.Parse()

	pc, err := net.ListenPacket("udp", localAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer pc.Close()

	buffer := make([]byte, 2048)
	var c client.Client
	c, err = client.NewHTTPClient(client.HTTPConfig{
		Addr:     influxAddress,
		Username: userName,
		Password: pass,
	})
	if err != nil {
		log.Fatal(err)
	}

	for {
		n, _, err := pc.ReadFrom(buffer)
		if err != nil {
			log.Fatal(err)
		}

		str := byteString(buffer)
		log.Println(str)

		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  "mi_sensor",
			Precision: "ns",
		})
		if err != nil {
			log.Fatal(err)
		}

		for i := 20; i < n; i += 40 {
			currentTimeStamp := int64(bytesToUint64(buffer[i : i+8]))
			currentTime := time.Unix(currentTimeStamp/1e9, currentTimeStamp%1e9)
			for j := 0; j < 8; j += 1 {
				tags := map[string]string{"id": str, "sensor": strconv.Itoa(j)}
				fields := map[string]interface{}{
					"mi_value": bytesToInt32(buffer[i+8+j*4 : i+8+j*4+4]),
				}
				pt, err := client.NewPoint("mi_sensor", tags, fields, currentTime)
				if err != nil {
					log.Fatal(err)
				}
				bp.AddPoint(pt)
			}
			//log.Println(bytesToInt32(buffer[i+12 : i+12+4]))
		}

		if err := c.Write(bp); err != nil {
			log.Println(err)
		}
	}
}

func byteString(p []byte) string {
	for i := 0; i < len(p); i++ {
		if p[i] == 0 {
			return string(p[0:i])
		}
	}
	return string(p)
}

func bytesToUint64(buf []byte) uint64 {
	return uint64(binary.LittleEndian.Uint64(buf))
}

func bytesToInt32(buf []byte) int32 {
	tmp := binary.LittleEndian.Uint32(buf)
	var res int32
	if tmp > 2^31 {
		res = int32(tmp - (2 ^ 32 - 1))
	} else {
		res = int32(tmp)
	}
	return res
}
