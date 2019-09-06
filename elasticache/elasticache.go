package elasticache

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
)

// Node is a single ElastiCache node
type Node struct {
	URL  string
	Host string
	IP   string
	Port int
}

// New returns an instance of the memcache client
func New(dsn string) (*memcache.Client, error) {
	urls, err := clusterNodes(dsn)
	if err != nil {
		return memcache.New(), err
	}

	return memcache.New(urls...), nil
}

func clusterNodes(dsn string) ([]string, error) {
	endpoint, err := elasticache(dsn)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		log.Printf("Socket Dial (%s): %s", endpoint, err.Error())
		return nil, err
	}
	defer conn.Close()

	command := "config get cluster\r\n"
	fmt.Fprintf(conn, command)

	response, err := parseNodes(conn)
	if err != nil {
		return nil, err
	}

	urls, err := parseURLs(response)
	if err != nil {
		return nil, err
	}

	return urls, nil
}

func elasticache(dsn string) (string, error) {
	if len(dsn) == 0 {
		dsn = os.Getenv("ELASTICACHE_ENDPOINT")
	}
	if len(dsn) == 0 {
		log.Println("ElastiCache endpoint not set")
		return "", errors.New("ElastiCache endpoint not set")
	}

	return dsn, nil
}

func parseNodes(conn io.Reader) (string, error) {
	var response string

	count := 0
	location := 3 // AWS docs suggest that nodes will always be listed on line 3

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		count++
		if count == location {
			response = scanner.Text()
		}
		if scanner.Text() == "END" {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println("Scanner: ", err.Error())
		return "", err
	}

	log.Println("ElastiCache nodes found: ", response)
	return response, nil
}

func parseURLs(response string) ([]string, error) {
	var urls []string
	var nodes []Node

	items := strings.Split(response, " ")

	for _, v := range items {
		fields := strings.Split(v, "|") // ["host", "ip", "port"]

		port, err := strconv.Atoi(fields[2])
		if err != nil {
			log.Println("Integer conversion: ", err.Error())
			return nil, err
		}

		node := Node{fmt.Sprintf("%s:%d", fields[1], port), fields[0], fields[1], port}
		nodes = append(nodes, node)
		urls = append(urls, node.URL)

		log.Printf("Host: %s, IP: %s, Port: %d, URL: %s", node.Host, node.IP, node.Port, node.URL)
	}

	return urls, nil
}
