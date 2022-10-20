package database

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/tidwall/sjson"
	nebula_sdk "github.com/vesoft-inc/nebula-go/v3"
	nb "github.com/vesoft-inc/nebula-go/v3/nebula"
)

func NewNebulaGraphConnection(address, hostPort string) (*nebula_sdk.ConnectionPool, error) {
	port, err := strconv.Atoi(hostPort)
	if err != nil {
		return nil, err
	}

	hostAddr := nebula_sdk.HostAddress{Host: address, Port: port}
	hostList := []nebula_sdk.HostAddress{hostAddr}
	// Create configs for connection pool using default values
	poolConfig := nebula_sdk.GetDefaultConf()
	// Initialize connection pool
	connPool, err := nebula_sdk.NewConnectionPool(hostList, poolConfig, nebula_sdk.DefaultLogger{})
	if err != nil {
		err = fmt.Errorf(fmt.Sprintf("Failed to initialize the connection pool : %v", err))
		return nil, err
	}
	return connPool, nil
}

func GetNebulaDBSession(connPool *nebula_sdk.ConnectionPool, spaceName, username, password string) (*nebula_sdk.Session, error) {
	// Create session
	session, err := connPool.GetSession(username, password)
	if err != nil {
		if strings.Contains(err.Error(), "Authentication") || strings.Contains(err.Error(), "authenticate") {
			err = fmt.Errorf("failed to login to the database for the username %s", username)
		} else {
			err = fmt.Errorf("internal error occured. Please contact the system administrator. %v", err)
		}

		return nil, err
	}

	//switch to user space, else continue to use the current space
	if spaceName != "" {
		var sb strings.Builder
		sb.WriteString("USE ")
		sb.WriteString(spaceName)
		sb.WriteString(";")
		// Execute the query
		_, err = session.Execute(sb.String())
		if err != nil {
			err = fmt.Errorf("internal error occured. Unable to use space: %s", spaceName)
			return nil, err
		}
	}

	return session, nil
}

func ParseAllPropsVertexResult(res *nebula_sdk.ResultSet) []string {
	var opResList []string

	for _, row := range res.GetRows() {
		// var rowJson string
		val := row.GetValues()[0]
		// vid := string(val.GetVVal().Vid.GetSVal())
		for _, tag := range val.GetVVal().GetTags() {
			var propJson string
			propJsonChan := make(chan string)
			properties := tag.GetProps()

			var wg1 sync.WaitGroup
			wg1.Add(len(properties))
			for k := range properties {

				var kjson string
				go func(k string) {
					kjson, _ = sjson.Set(propJson, string(k), getPropValue(properties[k]))
					propJsonChan <- kjson
					wg1.Done()
				}(k)
				propJson = <-propJsonChan
			}
			wg1.Wait()

			// rowJson, _ = sjson.Set(rowJson, "data", propJson)
			// rowJson, _ = sjson.Set(rowJson, "id", vid)
			opResList = append(opResList, propJson)
		}
	}

	return opResList
}

func getPropValue(val *nb.Value) interface{} {
	switch {
	case val.IsSetSVal():
		return string(val.GetSVal())
	case val.IsSetIVal():
		return val.GetIVal()
	case val.IsSetBVal():
		return val.GetBVal()
	case val.IsSetFVal():
		return val.GetFVal()
	case val.IsSetDtVal():
		return val.GetDtVal().String()
	case val.IsSetDVal():
		return val.GetDVal().String()
	}
	return ""
}
