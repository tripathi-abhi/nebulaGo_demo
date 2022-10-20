package main

import (
	"fmt"
	"log"
	nebula_db "nebulaoperations/database"
	"strings"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

const address = "a544bb691068b4de29168b8eacd18245-517439915.ap-south-1.elb.amazonaws.com"
const port = "9669"

func main() {
	sellocData, _ := FetchSellocData()
	fmt.Println("sellocData: ", sellocData.Names())
}

func FetchSellocData() (dataframe.DataFrame, error) {
	connPool, _ := nebula_db.NewNebulaGraphConnection(address, port)
	username := "root"
	password := "tul@1234"
	session, _ := nebula_db.GetNebulaDBSession(connPool, "Serviceability", username, password)

	defer session.Release()
	result, err := session.Execute("Match (s:Selloc) RETURN s")

	startTime := time.Now()

	if err != nil {
		log.Fatal(err)
	}

	rawSellocArray := nebula_db.ParseAllPropsVertexResult(result)
	rawSellocArrayJsonString := `[` + strings.Join(rawSellocArray, ",") + `]`
	df := dataframe.ReadJSON(strings.NewReader(rawSellocArrayJsonString))

	if df.Error() != nil {
		return df, df.Error()
	}

	df.SetNames([]string{"active", "city", "closingtime", "country", "store_id", "lphandovertimeedair", "lphandovertimeedsurface", "lphandovertimehdair", "lphandovertimehdsurface", "lphandovertimenddair", "lphandovertimenddsurface", "lphandovertimesddair", "lphandovertimesddsurface", "lphandovertimeslotair", "lphandovertimeslotsurface", "name", "openingtime", "orderacceptancetatedair", "orderacceptancetatedsurface", "orderacceptancetathdair", "orderacceptancetathdsurface", "orderacceptancetatnddair", "orderacceptancetatnddsurface", "orderacceptancetatsddair", "orderacceptancetatsddsurface", "orderacceptancetatslotair", "orderacceptancetatslotsurface", "ordercutofftimeedair", "ordercutofftimeedsurface", "ordercutofftimehdair", "ordercutofftimehdsurface", "ordercutofftimenddair", "ordercutofftimenddsurface", "ordercutofftimesddair", "ordercutofftimesddsurface", "ordercutofftimeslotair", "ordercutofftimeslotsurface", "orderprocessingtatedair", "orderprocessingtatedsurface", "orderprocessingtathdair", "orderprocessingtathdsurface", "orderprocessingtatnddair", "orderprocessingtatnddsurface", "orderprocessingtatsddair", "orderprocessingtatsddsurface", "orderprocessingtatslotair", "orderprocessingtatslotsurface", "pin", "returncity", "returncountry", "returnpin", "returnslaveid", "returnstate", "state", "workingdays"}...)

	df = df.Mutate(series.New(df.Col("ordercutofftimehdair").Records(), series.String, "ordercutofftimehd"))

	fmt.Println("Time elapsed: ", time.Since(startTime).Seconds())
	return df, nil
}
