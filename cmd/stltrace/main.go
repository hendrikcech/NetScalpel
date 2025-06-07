package main

import (
	"flag"
	"fmt"
	"log"
	"maps"
	"math"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"gitlab.lrz.de/cm/starlink/netmeas/pkg"
)

func main() {
	clientCmd := flag.NewFlagSet("client", flag.ExitOnError)
	clientIp := clientCmd.String("ip", "", "server ip")
	clientPort := clientCmd.Uint("port", 8500, "server port")
	clientResults := clientCmd.String("results", "results", "path to results folder")
	clientRounds := clientCmd.Uint("rounds", 0, "number of measurement rounds to run; 0 = infinite")
	clientProcedure := clientCmd.String("procedure", "trace", "test procedure")
	clientParams := clientCmd.String("params", "", "comma-separated key=value pairs passed to procedure")

	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	serverIp := serverCmd.String("ip", "0.0.0.0", "ip")
	serverPort := serverCmd.Uint("port", 8500, "port")

	if len(os.Args) < 2 {
		fmt.Println("expected 'client' or 'server' subcommands")
		os.Exit(1)
	}

	pkg.RegisterGob()

	go dumpOnSig()

	switch os.Args[1] {
	case "client":
		clientCmd.Parse(os.Args[2:])
		if *clientIp == "" {
			fmt.Println("expected -ip")
			os.Exit(1)
		}

		if *clientPort == 0 {
			fmt.Println("expected -port")
			os.Exit(1)
		}

		if *clientRounds == 0 {
			*clientRounds = math.MaxUint
		}

		params, err := parseParams(*clientParams)
		if err != nil {
			log.Fatalf("Failed parsing params: %v", err.Error())
		}

		client := Client{
			Ip:        *clientIp,
			Port:      *clientPort,
			Results:   *clientResults,
			Rounds:    *clientRounds,
			Procedure: *clientProcedure,
			Params:    params,
		}
		client.Run()

	case "server":
		serverCmd.Parse(os.Args[2:])

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		s := pkg.RunServer(*serverIp, *serverPort)

		sig := <-sigs
		fmt.Printf("Stopping server on %v\n", sig)
		s.Stop()

	default:
		fmt.Println("expected 'client' or 'server' subcommands")
		os.Exit(1)
	}
}

type ProcedureFunc func(time.Time, string, ParamMap)

type ParamMap map[string]any

func (p ParamMap) Direction() (pkg.Direction, error) {
	value, ok := p["direction"]
	if !ok {
		return 999, fmt.Errorf("Direction parameter not present")
	}
	directionStr, ok := value.(string)
	if !ok {
		return 999, fmt.Errorf("Direction parameter must be string")
	}
	direction, err := pkg.ParseDirection(directionStr)
	if err != nil {
		return 999, err
	}
	return direction, nil
}

// Parses comma-separated key=value pairs
// If value contains whitespace, value is parsed as a list
func parseParams(paramStr string) (ParamMap, error) {
	params := make(map[string]any)
	if paramStr == "" {
		return params, nil
	}
	for _, kv := range strings.Split(paramStr, ",") {
		parts := strings.Split(kv, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("Invalid key-value pair: %v", kv)
		}
		key := parts[0]
		value := parts[1]
		valueParts := strings.Split(value, " ")
		if len(valueParts) == 1 {
			params[key] = value
		} else {
			params[key] = valueParts
		}
	}
	return params, nil
}

type Client struct {
	Ip        string
	Port      uint
	Results   string
	Rounds    uint
	Procedure string
	Params    map[string]any

	round uint
}

func (c *Client) Run() {
	for i := range c.Rounds {
		e := NewExecutor(c.Ip, c.Port)
		now := time.Now()
		resultPath := ""
		c.round = i

		proceduresUlDl := map[string]ProcedureFunc{
			"burst":    e.BurstRi,
			"prograte": e.ProgressiveRate,
			"cooldown": e.CoolDown,
			"cdsf":     e.CoolDownSameFlow,
		}

		if fn, ok := proceduresUlDl[c.Procedure]; ok {
			if _, ok := c.Params["direction"]; ok {
				c.executeProcedure(now, fn, c.Params)
			} else {
				params := maps.Clone(c.Params)
				params["direction"] = pkg.UL.String()
				c.executeProcedure(now, fn, params)
				params["direction"] = pkg.DL.String()
				c.executeProcedure(now.Add(15*time.Second), fn, params)
			}
		} else if c.Procedure == "trace" {
			c.executeProcedure(now, fn, c.Params)
		} else {
			log.Fatalf("Unknown -procedure '%v'", c.Procedure)
		}

		if err := e.G.Wait(); err != nil {
			log.Fatalf("%v", err.Error())
		}

		log.Printf("Gathering results...")
		start := time.Now()
		if err := e.GatherResults(); err != nil {
			log.Fatalf("Failed gathering results: %v", err.Error())
		}
		duration := time.Now().Sub(start)
		log.Printf("Gathered results in %.2f seconds", duration.Seconds())

		if err := e.WriteInfo(resultPath); err != nil {
			log.Printf("Failed writing info: %v", err.Error())
		}
	}
}

func (c *Client) executeProcedure(ts time.Time, fn ProcedureFunc, params ParamMap) {
	ri := nextRi(ts)
	log.Printf("[Round %v] Schedule %s %s in %.2fs at %v", c.round, params["direction"], c.Procedure, ri.Sub(time.Now()).Seconds(), ri)
	name := "_" + c.Procedure
	if direction, ok := params["direction"]; ok {
		name += "_" + strings.ToLower(direction.(string))
	}
	resultPath := mkResultPath(c.Results, ri, name)
	fn(ri, resultPath, params)
}

func nextRi(ts time.Time) time.Time {
	base := ts.Round(time.Second)
	var riSec int
	for _, riSec = range []int{12, 27, 42, 57, 57 + 15} {
		if riSec > base.Second() {
			break
		}
	}
	minute := riSec / 60
	second := riSec % 60
	nextRi := time.Date(base.Year(), base.Month(), base.Day(), base.Hour(), base.Minute(), second, 0, base.Location())
	nextRi = nextRi.Add(time.Duration(minute) * time.Minute)
	return nextRi
}

func mkResultPath(base string, ts time.Time, suffix string) string {
	resultPath := filepath.Join(base, ts.Format("20060102T150405")+suffix)
	if err := os.MkdirAll(resultPath, os.ModePerm); err != nil {
		log.Fatalf("Failed to create the result directory %v: %v", resultPath, err)
	}
	return resultPath
}

func dialRpcClient(ip string, port uint) *rpc.Client {
	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		fmt.Printf("rpc.Dial failed: %v\n", err.Error())
		os.Exit(1)
	}
	return client
}

func dumpOnSig() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGQUIT)
	buf := make([]byte, 1<<20)
	for {
		<-sigs
		stacklen := runtime.Stack(buf, true)
		log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
	}
}
